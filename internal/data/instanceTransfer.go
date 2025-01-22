package data

import (
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

type InstanceTransfer struct {
	ID                 string
	NamingConvention   *NamingConvention
	SourceInstance     *Instance
	RestoredInstanceID string
	TargetType         string
	TargetHost         string
	TargetUsername     string
	TargetPassword     string
	CloudUsername      string
	CloudPassword      string
	Delimiter          string
	Newline            string
	Null               string
	PsqlAvailable      bool
	BcpAvailable       bool
	SqlLdrAvailable    bool
	TransferInfos      []*TransferInfo
	SchemaRootNode     *SchemaTree
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

// type InstanceTransfer struct {
// 	Id              string             `json:"id"`
// 	SourceName      string             `json:"source-name"`
// 	SourceType      string             `json:"source-type"`
// 	SourceHostname  string             `json:"source-hostname"`
// 	SourcePort      int                `json:"source-port"`
// 	SourceUsername  string             `json:"source-username"`
// 	SourcePassword  string             `json:"-"`
// 	SourceDatabase  string             `json:"source-database"`
// 	TargetName      string             `json:"target-name"`
// 	TargetType      string             `json:"target-type"`
// 	TargetHostname  string             `json:"target-hostname"`
// 	TargetPort      int                `json:"target-port"`
// 	TargetUsername  string             `json:"target-username"`
// 	TargetPassword  string             `json:"-"`
// 	TargetDatabase  string             `json:"target-database"`
// 	Delimiter       string             `json:"delimiter"`
// 	Newline         string             `json:"newline"`
// 	Null            string             `json:"null"`
// 	PsqlAvailable   bool               `json:"-"`
// 	BcpAvailable    bool               `json:"-"`
// 	SqlLdrAvailable bool               `json:"-"`
// 	CreatedAt       time.Time          `json:"created-at"`
// 	Context         context.Context    `json:"-"`
// 	Cancel          context.CancelFunc `json:"-"`
// 	TransferInfos   []TransferInfo     `json:"transfer-infos"`
// 	AccountID       string             `json:"account-id"`
// 	Region          string             `json:"region"`
// 	AccountUsername string             `json:"username"`
// 	AccountPassword string             `json:"-"`
// 	BackupId        string             `json:"backup-id"`
// }

func ValidateInstanceTransfer(v *validator.Validator, instanceTransfer *InstanceTransfer) {
}
