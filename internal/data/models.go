package data

import (
	"errors"

	"github.com/coreos/etcd/clientv3"
)

var (
	ErrRecordNotFound = errors.New("record not found")
	ErrEditConflict   = errors.New("edit conflict")
)

type Models struct {
	Permissions PermissionModel
	Tokens      TokenModel
	Users       UserModel
}

func NewModels(etcd *clientv3.Client) Models {
	return Models{
		Permissions: PermissionModel{Etcd: etcd},
		Tokens:      TokenModel{Etcd: etcd},
		Users:       UserModel{Etcd: etcd},
	}
}
