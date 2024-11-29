package commonHelpers

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

func CreateTransferTmpDirs(transferId, globalTmpDir string, logger *slog.Logger) (tmpDir, pipeFileDir, finalCsvDir string, err error) {
	tmpDir = filepath.Join(globalTmpDir, transferId)

	err = os.MkdirAll(tmpDir, 0600)
	if err != nil {
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating temp dir :: %v", err)
	}

	logger.Info(fmt.Sprintf("temp dir %v created", tmpDir))

	pipeFileDir = filepath.Join(tmpDir, "pipe-files")
	err = os.MkdirAll(pipeFileDir, 0600)
	if err != nil {
		go func() {
			err = os.RemoveAll(tmpDir)
			if err != nil {
				logger.Error(fmt.Sprintf("error removing temp dir %v :: %v", tmpDir, err))
				return
			}
			logger.Info(fmt.Sprintf("temp dir %v removed because pipe file dir creation failed", tmpDir))
		}()
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating pipe file dir :: %v", err)
	}

	logger.Info(fmt.Sprintf("pipe file dir %v created", pipeFileDir))

	finalCsvDir = filepath.Join(tmpDir, "final-csv")
	err = os.MkdirAll(finalCsvDir, 0600)
	if err != nil {
		go func() {
			err = os.RemoveAll(tmpDir)
			if err != nil {
				logger.Error(fmt.Sprintf("error removing temp dir %v :: %v", tmpDir, err))
				return
			}
			logger.Info(fmt.Sprintf("temp dir %v removed because final csv dir creation failed", tmpDir))
		}()
		return tmpDir, pipeFileDir, finalCsvDir, fmt.Errorf("error creating final csv dir :: %v", err)
	}

	logger.Info(fmt.Sprintf("final csv dir %v created", finalCsvDir))

	return tmpDir, pipeFileDir, finalCsvDir, nil
}
