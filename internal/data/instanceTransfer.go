package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type InstanceTransfer struct {
	ID                                  string
	NamingConvention                    *NamingConvention
	SourceInstance                      *Instance
	RestoredInstanceID                  string
	TargetType                          string
	TargetHost                          string
	TargetUsername                      string
	TargetPassword                      string
	CloudUsername                       string
	CloudPassword                       string
	Delimiter                           string
	Newline                             string
	Null                                string
	PsqlAvailable                       bool
	BcpAvailable                        bool
	SqlLdrAvailable                     bool
	TransferInfos                       []*TransferInfo
	SchemaTree                          *SafeTreeNode
	ScanForPII                          bool
	StagingDbNames                      []string
	DeleteRestoredInstanceAfterTransfer bool
}

var (
	StatusPending             = "Pending"
	StatusRestoring           = "Restoring backup"
	StatusBackupRestored      = "Backup restored"
	StatusChangingCredentials = "Changing credentials"
	StatusMovingData          = "Moving data"
	StatusComplete            = "Complete"
	StatusError               = "Error"
)

func ValidateInstanceTransfer(v *validator.Validator, instanceTransfer *InstanceTransfer) {
}
