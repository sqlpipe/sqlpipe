package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/cobra"
	"github.com/sqlpipe/sqlpipe/internal/data"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

var (
	InitializeCmd = &cobra.Command{
		Use:   "initialize",
		Short: "Prepare a new etcd cluster for usage by SQLpipe.",
		Long:  "Prepare a new etcd cluster for usage by SQLpipe. Must have authentication disabled. SQLpipe will create a user, role, and directory called sqlpipe, and grant the user access to the role, and the role access to that directory. It will also give the root user a password, and enable authentication.",
		Run:   initializeCmd,
	}
	etcdEndpoints        []string
	etcdRootPassword     string
	etcdSqlpipePassword  string
	sqlpipeAdminPassword string
)

func init() {
	InitializeCmd.Flags().StringVar(&etcdRootPassword, "etcd-root-password", "", "etcd root password")
	InitializeCmd.Flags().StringVar(&etcdSqlpipePassword, "etcd-sqlpipe-password", "", "password for new 'sqlpipe' user in etcd")
	InitializeCmd.Flags().StringVar(&sqlpipeAdminPassword, "sqlpipe-admin-password", "", "password for new 'admin' user in sqlpipe")
	InitializeCmd.Flags().StringSliceVar(&etcdEndpoints, "etcd-endpoints", []string{}, "etcd endpoints, comma separated no spaces")
}

func initializeCmd(cmd *cobra.Command, args []string) {
	if reflect.DeepEqual(etcdEndpoints, []string{}) {
		log.Fatal(errors.New("--etcd-cluster flag given without specifying cluster endpoints"))
	}

	if etcdRootPassword == "" {
		log.Fatal("--etcd-root-password empty")
	}

	if etcdSqlpipePassword == "" {
		log.Fatal("--etcd-sqlpipe-password empty")
	}

	if sqlpipeAdminPassword == "" {
		log.Fatal("--sqlpipe-admin-password empty")
	}

	user := &data.User{
		Username: "admin",
		Admin:    true,
	}

	err = user.SetPassword(sqlpipeAdminPassword)
	if err != nil {
		log.Fatal(err)
	}

	v := validator.New()

	if data.ValidateUser(v, user); !v.Valid() {
		log.Fatal(v.Errors)
	}

	globals.EtcdTimeout = time.Second * 5

	etcd, err := clientv3.New(
		clientv3.Config{
			Endpoints:   etcdEndpoints,
			DialTimeout: time.Second * 5,
		},
	)
	if err != nil {
		log.Fatal(
			errors.New("unable to connect to etcd"),
		)
	}
	defer etcd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := etcd.Get(ctx, "sqlpipe")
	if err != nil {
		log.Fatal(err)
	}
	if resp.Count != 0 {
		log.Fatal("there is already a sqlpipe directory initialized in this etcd cluster. to initialize, you must delete the sqlpipe node, all children nodes, sqlpipe user, and sqlpipe role")
	}

	_, err = etcd.Put(
		ctx,
		"sqlpipe",
		fmt.Sprint(time.Now()),
	)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = etcd.RoleAdd(ctx, "root"); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserAdd(ctx, "root", etcdRootPassword); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserGrantRole(ctx, "root", "root"); err != nil {
		log.Fatal(err)
	}

	if _, err = etcd.RoleAdd(ctx, "sqlpipe"); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserAdd(ctx, "sqlpipe", etcdSqlpipePassword); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserGrantRole(ctx, "sqlpipe", "sqlpipe"); err != nil {
		log.Fatal(err)
	}

	if _, err = etcd.RoleGrantPermission(
		ctx,
		"sqlpipe",
		"sqlpipe",
		"sqlpipf",
		clientv3.PermissionType(clientv3.PermReadWrite),
	); err != nil {
		log.Fatal(err)
	}

	m := data.NewModels(etcd)

	err = m.Users.Insert(user)

	if err != nil {
		switch {
		case errors.Is(err, data.ErrDuplicateUsername):
			v.AddError("username", "a user with this username already exists")
			log.Fatal(v.Errors)
		default:
			log.Fatal(v.Errors)
		}
	}

	if _, err = etcd.AuthEnable(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("initialized etcd cluster for usage with SQLpipe, and created a SQLpipe user called 'admin'")
}
