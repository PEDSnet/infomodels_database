package database

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/infomodels/datadirectory"
	"github.com/lib/pq" // PostgreSQL database driver
	log "github.com/Sirupsen/logrus"
  "io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
)

// columnNamesFromCsvFile returns the column headings from the CSV `fileName`.
func columnNamesFromCsvFile(fileName string) ([]string, error) {
	fileReader, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	csvReader := csv.NewReader(fileReader)

	record, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("Error reading first row of `%s`: %v", fileName, err)
	}
	return record, nil
}

// lineCounter counts the number of physical text lines returned by a Reader.
// See http://stackoverflow.com/a/24563853/390663.
// As long as our csv files are not allowed to include newlines in
// fields, this approach is legitimate. If the final line is not
// terminated by a newline, it is still counted.
func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}
  var lastByte byte
  lastByte = '\n'
  
	for {
		c, err := r.Read(buf)
    if c > 0 {
      lastByte = buf[c - 1]
    }
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			if lastByte != '\n' {
        log.Warn(fmt.Sprintf("Last byte in buffer is '%v'", lastByte))
				count += 1
			}
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// rowsInFile returns the number of physical lines in a file.
func rowsInFile(fileName string) (int, error) {
	fileReader, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	defer fileReader.Close()
	return lineCounter(fileReader)
}

func rowsInTable(conn_str string, schema string, table string) (int, error) {
	var count int
  // TODO: pass in the driver name
  db, err := openDatabase("postgres", conn_str)
  if err != nil {
    return 0, err
  }
  
	sql := fmt.Sprintf("select count(*) as count from %s.%s", schema, table)
	err = db.QueryRow(sql).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("Can't get count of %s.%s table: %v", schema, table, err)
	}
	return count, nil
}

type CopyCommandArgs struct {
	DatabaseUrl string
	Schema      string
	Table       string
	CsvFile     string
	WaitGroup   sync.WaitGroup
}

// copyCommand returns an exec.Command for loading a CSV data file into a database using `psql` via the shell.
// CSV files are assumed to be named {table}.csv within a top-level directory in the zip file.
// The column names are first extracted from the CSV file so we assign columns in the CSV file to the correct columns in the table.
func copyCommand(databaseUrl string, schema string, table string, csvFile string, wg sync.WaitGroup) error {

	columnNames, err := columnNamesFromCsvFile(csvFile)
	if err != nil {
		return err
	}

	if _, err := exec.LookPath("psql"); err != nil {
		return fmt.Errorf("`psql` binary must be in PATH")
	}

	columns := strings.Join(columnNames, ", ")

	connectionString, err := pq.ParseURL(databaseUrl)
	if err != nil {
		return fmt.Errorf("Invalid database URL: %v", databaseUrl)
	}

	cmdStr := fmt.Sprintf(`psql "%s" -c "\COPY %s.%s(%s) FROM '%s' (FORMAT csv, HEADER true, ENCODING 'utf-8', FORCE_NULL(%s))"`, connectionString, schema, table, columns, csvFile, columns)

	cmd := exec.Command("sh", "-c", cmdStr)
  
	var e bytes.Buffer
	cmd.Stderr = &e

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("Error running command with `sh -c`: %v (STDERR: %s)", cmdStr, err, string(e.Bytes()))
	}

	actualRows, err := rowsInTable(connectionString, schema, table)
  if err != nil {
		return fmt.Errorf("Load for %s.%s nominally worked, but counting the number of rows failed: %v", schema, table, err)
	}

	expectedRows, err := rowsInFile(csvFile)
  expectedRows -= 1   // Account for header
  if err != nil {
		return fmt.Errorf("Load for %s.%s nominally worked, but counting the number of lines in the csv file failed: %v", schema, table, err)
	}

	if actualRows != expectedRows {
    err = fmt.Errorf("Number of rows in %s.%s (%d) does not equal the number of lines (%d) in the input file", schema, table, actualRows, expectedRows)
    log.Error(fmt.Sprintf("In copyCommand: %v", err))
		return err
	}

	log.Info(fmt.Sprintf("Loaded %d rows into %s.%s", actualRows, schema, table))

	return nil
}

// versionToShorthand - given a version string such as "X.Y.Z", return "XY"
// TODO: this is an unscalable convention, obviously
func versionToShorthand(version string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", fmt.Errorf("Version string must be like X.Y or X.Y.Z, not '%s'", version)
	}
	return parts[0] + parts[1], nil
}

// databaseName returns a database name, given a version string, e.g. '21' for '2.1' or '2.1.3'
// `modelVersion` is the PEDSnet model version: X.Y.Z or X.Y
func databaseName(modelVersion string) (shortVersion string, err error) {
	if shortVersion, err = versionToShorthand(modelVersion); err != nil {
		return
	}
	return fmt.Sprintf("pedsnet_dcc_v%s", shortVersion), nil
}

// load does the work for Load below
func (d *Database) load(datadirectory *datadirectory.DataDirectory) error {
	var err error

	// We will parallelize our loads, using a concurrency of 4, or the number in the PREPDB_JOBS environment variable
	tasks := make(chan *CopyCommandArgs, 100) // 100 is an impossibly large number of vocab files
	taskErrors := make(chan error, 100)

	numJobs := 4
	numJobsStr := os.Getenv("DATABASE_LOAD_JOBS")
	if numJobsStr != "" {
		numJobs, err = strconv.Atoi(numJobsStr)
		if err != nil || !(numJobs > 0) {
			return fmt.Errorf("DATABASE_LOAD_JOBS environment variable has invalid positive integer")
		}
	}

	// spawn worker goroutines and define our worker function
	var wg sync.WaitGroup
	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(n int) {
			for args := range tasks {
				err := copyCommand(args.DatabaseUrl, args.Schema, args.Table, args.CsvFile, args.WaitGroup)
				if err != nil {
					taskErrors <- err
				}
			}
			wg.Done()
		}(i)
	}

	// Now create our loading tasks by iterating through the datadirectory metadata/manifest

	for _, m := range datadirectory.RecordMaps {
		table := m["table"]
		fileName := path.Join(datadirectory.DirPath, m["filename"])
		copyArgs := &CopyCommandArgs{
			DatabaseUrl: d.DatabaseUrl,
			Schema:      d.Schema,
			Table:       table,
			CsvFile:     fileName,
			WaitGroup:   wg}
		tasks <- copyArgs
	} // end for all files

	close(tasks) // This will cause the channel receivers (tasks) to finish their range loops

	wg.Wait()
	close(taskErrors)

	masterError := ""
	for err := range taskErrors {
		masterError += err.Error() + "\n"
	}
	if masterError != "" {
		masterError += "\n"
	}
	if masterError != "" {
		return fmt.Errorf(masterError)
	}

	return nil
} // end load

// Load populates data model tables by shelling out to psql.
// `dataDirectory` specifies a directory of CSV files and a manifest file that maps tables to files.
func (d *Database) Load(dataDirectory *datadirectory.DataDirectory) (err error) {
	return d.load(dataDirectory)
}
