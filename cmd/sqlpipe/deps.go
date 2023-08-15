package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func loadDeps() error {
	var err error
	switch platform {
	case "linux-amd64":
		err = loadPsqlLinuxAmd64()
		if err != nil {
			return fmt.Errorf("failed to load psql :: %v", err)
		}
		err = loadBcpLinuxAmd64()
		if err != nil {
			return fmt.Errorf("failed to load bcp :: %v", err)
		}
		// err = loadMysqlshLinuxAmd64()
		// if err != nil {
		// 	return fmt.Errorf("failed to load mysqlsh :: %v", err)
		// }
	}

	return nil
}

func loadPsqlLinuxAmd64() error {
	var err error
	psqlTmpFile, err = os.CreateTemp("", "")
	if err != nil {
		errorLog.Fatalf("failed to create psql temp file :: %v", err)
	}
	defer psqlTmpFile.Close()

	psqlBytes, err := fs.ReadFile(depsFs, "deps/psql/linux-amd-64/psql")
	if err != nil {
		errorLog.Fatalf("failed to read psql bytes :: %v", err)
	}

	_, err = psqlTmpFile.Write(psqlBytes)
	if err != nil {
		log.Fatalf("failed to write psql bytes :: %v", err)
	}

	err = psqlTmpFile.Close()
	if err != nil {
		log.Fatalf("failed to close psql file :: %v", err)
	}

	err = os.Chmod(psqlTmpFile.Name(), 0755)
	if err != nil {
		log.Fatalf("failed to change psql permissions :: %v", err)
	}

	// get combined output
	output, err := exec.Command(psqlTmpFile.Name(), "--version").CombinedOutput()
	if err != nil {
		errorLog.Fatalf("unable to check psql installation :: %v :: %v", err, string(output))
	}

	psqlAvailable = true

	return nil
}

func loadBcpLinuxAmd64() error {
	bcpTmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		errorLog.Fatalf("failed to create bcp temp dir :: %v", err)
	}

	err = copyDir(depsFs, "deps/bcp/linux-amd-64", bcpTmpDir)
	if err != nil {
		log.Fatalf("Failed to copy bcp linux amd 64 deps: %v", err)
	}

	bcpTmpFilePath := filepath.Join(bcpTmpDir, "bin/bcp")

	// get combined output
	output, err := exec.Command(bcpTmpFilePath, "-v").CombinedOutput()
	if err != nil {
		errorLog.Fatalf("unable to check bcp installation :: %v :: %v", err, string(output))
	}

	bcpTmpFile, err = os.Open(bcpTmpFilePath)
	if err != nil {
		errorLog.Fatalf("failed to open bcp temp file :: %v", err)
	}
	defer bcpTmpFile.Close()

	bcpAvailable = true

	return nil
}

// func loadMysqlshLinuxAmd64() error {
// 	mysqlshTmpDir, err := os.MkdirTemp("", "")
// 	if err != nil {
// 		errorLog.Fatalf("failed to create mysqlsh temp dir :: %v", err)
// 	}

// 	err = copyDir(depsFs, "deps/mysqlsh/linux-amd-64", mysqlshTmpDir)
// 	if err != nil {
// 		log.Fatalf("Failed to copy mysqlsh linux amd 64 deps: %v", err)
// 	}

// 	mysqlshTmpFilePath := filepath.Join(mysqlshTmpDir, "bin/mysqlsh")

// 	// get combined output
// 	output, err := exec.Command(mysqlshTmpFilePath, "--version").CombinedOutput()
// 	if err != nil {
// 		errorLog.Fatalf("unable to check mysqlsh installation :: %v :: %v", err, string(output))
// 	}

// 	mysqlshTmpFile, err = os.Open(mysqlshTmpFilePath)
// 	if err != nil {
// 		errorLog.Fatalf("failed to open mysqlsh temp file :: %v", err)
// 	}
// 	defer mysqlshTmpFile.Close()

// 	mysqlshAvailable = true

// 	return nil
// }

func copyDir(fsys fs.FS, src string, dst string) error {
	return fs.WalkDir(fsys, src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relativePath, _ := filepath.Rel(src, path)
		hostPath := filepath.Join(dst, relativePath)

		if d.IsDir() {
			return os.MkdirAll(hostPath, 0755)
		}

		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		return os.WriteFile(hostPath, data, 0744)
	})
}
