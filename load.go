package database

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/infomodels/datadirectory"
	"github.com/lib/pq" // PostgreSQL database driver
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

// copyCommand returns an exec.Command for loading a CSV data file into a database using `psql` via the shell.
// CSV files are assumed to be named {table}.csv within a top-level directory in the zip file.
// The column names are first extracted from the CSV file so we assign columns in the CSV file to the correct columns in the table.
func copyCommand(databaseUrl string, schema string, table string, csvFile string, wg sync.WaitGroup) (*exec.Cmd, error) {

	columnNames, err := columnNamesFromCsvFile(csvFile)
	if err != nil {
		return nil, err
	}

	if _, err := exec.LookPath("psql"); err != nil {
		return nil, fmt.Errorf("`psql` binary must be in PATH")
	}

	columns := strings.Join(columnNames, ", ")

	connectionString, err := pq.ParseURL(databaseUrl)
	if err != nil {
		return nil, fmt.Errorf("Invalid database URL: %v", databaseUrl)
	}

	var cmd = fmt.Sprintf(`psql "%s" -c "COPY %s.%s(%s) FROM '%s' (FORMAT csv, HEADER true, ENCODING 'utf-8')"`, connectionString, schema, table, columns, csvFile)
	return exec.Command("sh", "-c", cmd), nil
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
	tasks := make(chan *exec.Cmd, 100) // 100 is an impossibly large number of vocab files
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
			for cmd := range tasks {
				var e bytes.Buffer
				cmd.Stderr = &e
				if err := cmd.Run(); err != nil {
					taskErrors <- fmt.Errorf("Error running command: %s: %v (%s)", strings.Join(cmd.Args, " "), err, string(e.Bytes()))
				}
			}
			wg.Done()
		}(i)
	}

	// Now create our loading tasks by iterating through the datadirectory metadata/manifest

	for _, m := range datadirectory.RecordMaps {
		table := m["table"]
		fileName := path.Join(datadirectory.DirPath, m["filename"])
		cmd, err := copyCommand(d.DatabaseUrl, d.Schema, table, fileName, wg)
		if err != nil {
			return fmt.Errorf("load: error in copyCommand: %v", err)
		}
		tasks <- cmd
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
