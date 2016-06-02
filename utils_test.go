package database

// Utilities used by tests

import (
	"archive/zip"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// downloadFile creates a file and downloads a URL to it
// http://stackoverflow.com/a/33853856/390663
func downloadFile(filePath string, url string) (err error) {

	// Create the file
	out, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer out.Close()

	var srcReader io.ReadCloser

	if strings.HasPrefix(url, "file://") {

		sourceFileName := strings.TrimPrefix(url, "file://")
		srcReader, err = os.Open(sourceFileName)
		if err != nil {
			return err
		}
		defer srcReader.Close()

	} else {

		// Get the data
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		srcReader = resp.Body
		defer resp.Body.Close()
	}

	// Writer the body to file
	_, err = io.Copy(out, srcReader)
	if err != nil {
		return err
	}

	return nil
}

// unzip recursively unzips a zip archive into a target `dest` directory (created if needed).
// Stolen from http://stackoverflow.com/a/24792688/390663
func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}
