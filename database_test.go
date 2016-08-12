package database

import (
	"database/sql"
	"fmt"
	"github.com/infomodels/datadirectory"
	"github.com/infomodels/datapackage"
	"io/ioutil"
	//	log "github.com/Sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type TestEnv struct {
	DatabaseUrl, SearchPath, DmsaUrl, DmUrl, Model, ModelVersion, TempDir string
	PedsnetVocabDataDir                                                   *datadirectory.DataDirectory
}

// A zip file containing some test vocab data
var pedsnetVocabUrl = "http://github.com/infomodels/database/test_resources/pedsnet_vocab.tar.gz"

// NewTestEnv initializes the test environment from environment variables.
//
// Call the Cleanup() method afterwards to remove temp files.
//
// The `DT_DATABASE_URL` variable is required.
//
// The optional variable `DT_DMSA_URL` allows overriding the default
// of http://data-models-sqlalchemy.research.chop.edu/.
//
// The optional variable `DT_DM_URL` allows overriding the default
// of http://data-models-service.research.chop.edu/.
//
// The optional variable `DT_VOCAB_URL` allows overriding the default of
// http://github.com/infomodels/database/test_resources/pedsnet_vocab.tar.gz.
// `DT_VOCAB_URL` can also be an absolute local file name.
func NewTestEnv(t *testing.T) *TestEnv {

	te := new(TestEnv)

	if te.DatabaseUrl = os.Getenv("DT_DATABASE_URL"); te.DatabaseUrl == "" {
		t.Error("DT_DATABASE_URL environment variable required for testing")
		t.FailNow()
	}

	te.DmsaUrl = os.Getenv("DT_DMSA_URL")
	if te.DmUrl == "" {
		te.DmUrl = "http://data-models-sqlalchemy.research.chop.edu/"
	}

	te.DmUrl = os.Getenv("DT_DM_URL")
	if te.DmUrl == "" {
		te.DmUrl = "http://data-models-service.research.chop.edu/"
	}

	var err error
	te.TempDir, err = ioutil.TempDir("", "testdatadir")
	if err != nil {
		t.Error(fmt.Sprintf("TempDir failed: %v", err))
		t.FailNow()
	}

	vocabUrl := os.Getenv("DT_VOCAB_URL")
	if vocabUrl != "" {
		pedsnetVocabUrl = vocabUrl
	}

	var pedsnetVocabZipFile = filepath.Join(te.TempDir, "pedsnet_vocab.tar.gz")

	if strings.HasPrefix(pedsnetVocabUrl, "http") {
		if err = downloadFile(pedsnetVocabZipFile, pedsnetVocabUrl); err != nil {
			t.Error(fmt.Sprintf("Failed to download '%s' to a temp file: %v", pedsnetVocabUrl, err))
			t.FailNow()
		}
	} else {
		// Assume DT_VOCAB_URL is an absolute local file name
		os.Link(pedsnetVocabUrl, pedsnetVocabZipFile)
	}

	pedsnetVocabDataDirPath := filepath.Join(te.TempDir, "pedsnet_vocab")

	pkg := datapackage.DataPackage{PackagePath: pedsnetVocabZipFile}
	if err = pkg.Unpack(pedsnetVocabDataDirPath); err != nil {
		t.Error(fmt.Sprintf("Failed to unpack '%s': %v", pedsnetVocabUrl, err))
		t.FailNow()
	}

	cfg := &datadirectory.Config{
		DataDirPath: pedsnetVocabDataDirPath,
		Service:     te.DmUrl,
	}

	if te.PedsnetVocabDataDir, err = datadirectory.New(cfg); err != nil {
		t.Error(fmt.Sprintf("Failed to create new PEDSnet vocab DataDirectory object: %v", err))
		t.FailNow()
	}

	err = te.PedsnetVocabDataDir.ReadMetadataFromFile()
	if err != nil {
		t.Error(fmt.Sprintf("Failed to read metadata from file: %v", err))
		t.FailNow()
	}

	return te
}

func (te *TestEnv) Cleanup() {
	os.RemoveAll(te.TempDir)
}

// execSql executes a non-SELECT SQL statement
func execSql(db *sql.DB, sql string) error {
	_, err := db.Exec(sql)
	if err != nil {
		return fmt.Errorf("Error running `%s`: %v", sql, err)
	}
	return nil
}

// Create a database schema for postgres
func createSchema(db *sql.DB, schema string) error {
	var sql = fmt.Sprintf("create schema %s", schema)
	err := execSql(db, sql)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("Can't run test if `%s` schema already exists", schema)
		}
	}
	return err
}

// dropSchema drops a database schema for postgres.
// The `cascade` option is used, so this will blow away a schema even if it contains data.
func dropSchema(db *sql.DB, schema string) error {
	return execSql(db, fmt.Sprintf("drop schema %s cascade", schema))
}

// assertNoErrors executes a database command with the passed error handling mode ("strict", "normal", or "force")
// and fails if an error is returned
func assertNoErrors(t *testing.T, f func(string) error, funcName string, errorMode string) {
	if err := f(errorMode); err != nil {
		t.Error(fmt.Sprintf("%s failed: %v", funcName, err))
		t.FailNow()
	}
}

// introspectTables returns the tables in a specified `schema` using database handle `db`.
// The table names are returned as keys of a map[string]bool.
func introspectTables(t *testing.T, db *sql.DB, schema string) map[string]bool {
	sql := "select table_name from information_schema.tables where table_schema = $1"
	rows, err := db.Query(sql, schema)
	if err != nil {
		t.Error(fmt.Sprintf("db.Query failed for `%s`: %v", sql, err))
		t.FailNow()
	}
	defer rows.Close()
	tables := make(map[string]bool)
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Error(fmt.Sprintf("rows.Scan failed for `%s`: %v", sql, err))
			t.FailNow()
		}
		tables[table] = true
	}
	if err := rows.Err(); err != nil {
		t.Error(fmt.Sprintf("rows.Err returned error for `%s`: %v", sql, err))
		t.FailNow()
	}
	return tables
}

func mapContainsValues(has map[string]bool, values []string) bool {
	for _, val := range values {
		if !has[val] {
			return false
		}
	}
	return true
}

var pedsnetVocabDataModel string = "pedsnet-vocab"
var pedsnetCoreDataModel string = "pedsnet-core"
var pedsnetDataModelVersion string = "2.2.0"
var pedsnetVocabSearchPath string = "dt_pedsnet_vocab"
var pedsnetCoreSearchPath string = "dt_pedsnet_core,dt_pedsnet_vocab"

// // keysInMap returns a slice containing the keys from a map whose keys are strings.
// func keysInMap(m map[string]string) []string {
//   keys := make([]string, len(m))
//   i := 0
//   for k := range m {
//     keys[i] = k
//     i++
//   }
//   return keys
// }

// Note: searchPath may be a comma-separated list of schemas, the first of which is primary, as in PostgreSQL
func instantiateDataModel(t *testing.T, te *TestEnv, dataModel string, dataModelVersion string, searchPath string, verifyTables []string, shouldLoad bool) {
	var (
		err error
		d   *Database
	)
	if d, err = Open(dataModel, dataModelVersion, te.DatabaseUrl, searchPath, te.DmsaUrl, "", ""); err != nil {
		t.Error(fmt.Sprintf("Open failed: %v", err))
		t.FailNow()
	}
	defer d.Close()

	primarySchema, err := primarySchemaInSearchPath(searchPath)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if err = createSchema(d.db, primarySchema); err != nil {
		t.Error(fmt.Sprintf("Tests require the ability to create schemas: %v", err))
		t.FailNow()
	}

	assertNoErrors(t, d.CreateTables, "CreateTables", "strict")
	// And make sure that "normal" mode works: 'already exists' is benign:
	assertNoErrors(t, d.CreateTables, "CreateTables", "normal")

	tables := introspectTables(t, d.db, primarySchema)
	if !mapContainsValues(tables, verifyTables) {
		t.Error("Table creation failed:")
		t.Error(fmt.Sprintf("Expected tables: %v", verifyTables))
		t.Error(fmt.Sprintf("Candidate tables: %v", tables))
		t.FailNow()
	}

	if shouldLoad {
		loadDataModel(t, te, pedsnetVocabDataModel, pedsnetDataModelVersion, pedsnetVocabSearchPath, verifyTables)
	}

	// TODO: verify these operations also:
	assertNoErrors(t, d.CreateIndexes, "CreateIndexes", "strict")
	// Ensure that double-creation of indexes doesn't cause an error in "normal" errorMode
	assertNoErrors(t, d.CreateIndexes, "CreateIndexes", "normal")
	assertNoErrors(t, d.CreateConstraints, "CreateConstraints", "strict")
	// Ensure that double-creation of constraints doesn't cause an error in "normal" errorMode
	assertNoErrors(t, d.CreateConstraints, "CreateConstraints", "normal")
} // end func instantiateDataModel

// Note: searchPath may be a comma-separated list of schemas, the first of which is primary, as in PostgreSQL
func loadDataModel(t *testing.T, te *TestEnv, dataModel string, dataModelVersion string, searchPath string, verifyTables []string) {
	var (
		err           error
		d             *Database
		primarySchema string
	)
	if d, err = Open(dataModel, dataModelVersion, te.DatabaseUrl, searchPath, te.DmsaUrl, "", ""); err != nil {
		t.Error(fmt.Sprintf("Open failed: %v", err))
		t.FailNow()
	}
	defer d.Close()

	d.Load(te.PedsnetVocabDataDir)

	primarySchema, err = primarySchemaInSearchPath(searchPath)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	var count int
	sql := fmt.Sprintf("select count(*) as count from %s.concept", primarySchema)
	err = d.db.QueryRow(sql).Scan(&count)
	if err != nil {
		t.Error(fmt.Sprintf("Can't get count of concept table: %v", err))
		t.FailNow()
	}
	expected := 1148
	if count != expected {
		t.Error(fmt.Sprintf("Count of rows in concept table incorrect (%d, should be %d)", count, expected))
		t.FailNow()
	}
} // end func loadDataModel

func instantiatePedsnetVocab(t *testing.T, te *TestEnv) {
	verifyTables := []string{"concept", "concept_ancestor", "concept_class", "concept_relationship", "concept_synonym", "domain", "drug_strength", "relationship", "source_to_concept_map", "version_history", "vocabulary"}
	instantiateDataModel(t, te, pedsnetVocabDataModel, pedsnetDataModelVersion, pedsnetVocabSearchPath, verifyTables, true)
}

func instantiatePedsnetCore(t *testing.T, te *TestEnv) {
	verifyTables := []string{
		"care_site",
		"condition_occurrence",
		"death",
		"drug_exposure",
		"fact_relationship",
		"location",
		"measurement",
		"measurement_organism",
		"observation",
		"observation_period",
		"person",
		"procedure_occurrence",
		"provider",
		"version_history",
		"visit_occurrence",
		"visit_payer",
	}
	instantiateDataModel(t, te, pedsnetCoreDataModel, pedsnetDataModelVersion, pedsnetCoreSearchPath, verifyTables, false)
}

func destroyDataModel(t *testing.T, te *TestEnv, dataModel string, dataModelVersion string, searchPath string) {
	var (
		err error
		d   *Database
	)
	if d, err = Open(dataModel, dataModelVersion, te.DatabaseUrl, searchPath, te.DmsaUrl, "", ""); err != nil {
		t.Error(fmt.Sprintf("Open failed: %v", err))
		t.FailNow()
	}
	defer d.Close()

	// TODO: verify these operations
	assertNoErrors(t, d.DropConstraints, "DropConstraints", "strict")
	assertNoErrors(t, d.DropConstraints, "DropConstraints", "normal")
	assertNoErrors(t, d.DropIndexes, "DropIndexes", "strict")
	assertNoErrors(t, d.DropIndexes, "DropIndexes", "normal")
	assertNoErrors(t, d.DropTables, "DropTables", "strict")
	assertNoErrors(t, d.DropTables, "DropTables", "normal")

	primarySchema, err := primarySchemaInSearchPath(d.SearchPath)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	if err = dropSchema(d.db, primarySchema); err != nil {
		t.Error(fmt.Sprintf("Warning: dropping test schema %s failed: %v", primarySchema, err))
		t.FailNow()
	}
}

func destroyPedsnetVocab(t *testing.T, te *TestEnv) {
	destroyDataModel(t, te, pedsnetVocabDataModel, pedsnetDataModelVersion, pedsnetVocabSearchPath)
}

func destroyPedsnetCore(t *testing.T, te *TestEnv) {
	destroyDataModel(t, te, pedsnetCoreDataModel, pedsnetDataModelVersion, pedsnetCoreSearchPath)
}

// TestPedsnetInParts tests the 'pedsnet-core' data model, which is
// assumed to not include the vocabulary tables (but those must exist
// in the schema search path) and the 'pedsnet-vocab' data model,
// which is assumed to include just the vocabulary tables.
func TestPedsnetInParts(t *testing.T) {

	te := NewTestEnv(t)

	instantiatePedsnetVocab(t, te)
	instantiatePedsnetCore(t, te)
	destroyPedsnetCore(t, te)
	destroyPedsnetVocab(t, te)

	te.Cleanup()
}
