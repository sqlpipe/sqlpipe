package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
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
	InitializeCmd.Flags().StringSliceVar(&etcdEndpoints, "etcd-endpoints", []string{}, "etcd endpoints, comma separated no spaces")
	InitializeCmd.Flags().StringVar(&etcdRootPassword, "etcd-root-password", "", "etcd root password")
	InitializeCmd.Flags().StringVar(&etcdSqlpipePassword, "etcd-sqlpipe-password", "", "password for new 'sqlpipe' user in etcd")
	InitializeCmd.Flags().StringVar(&sqlpipeAdminPassword, "sqlpipe-admin-password", "", "password for new 'admin' user in sqlpipe")
}

func initializeCmd(cmd *cobra.Command, args []string) {
	v := validator.New()

	v.Check(etcdRootPassword != "", "--etcd-root-password", "must be provided")
	v.Check(len([]rune(etcdRootPassword)) >= 12, "--etcd-root-password", "must be at least 12 characters long")
	v.Check(len([]rune(etcdRootPassword)) <= 32, "--etcd-root-password", "must not be more than 32 characters long")

	v.Check(etcdSqlpipePassword != "", "--etcd-sqlpipe-password", "must be provided")
	v.Check(len([]rune(etcdSqlpipePassword)) >= 12, "--etcd-sqlpipe-password", "must be at least 12 characters long")
	v.Check(len([]rune(etcdSqlpipePassword)) <= 32, "--etcd-sqlpipe-password", "must not be more than 32 characters long")

	v.Check(sqlpipeAdminPassword != "", "--sqlpipe-admin-password", "must be provided")
	v.Check(len([]rune(sqlpipeAdminPassword)) >= 12, "--sqlpipe-admin-password", "must be at least 12 characters long")
	v.Check(len([]rune(sqlpipeAdminPassword)) <= 32, "--sqlpipe-admin-password", "must not be more than 32 characters long")

	v.Check(len(etcdEndpoints) != 0, "--etcd-endpoints", "must be provided")

	user := &data.User{
		Username: "admin",
		Admin:    true,
	}

	if err = user.SetPassword(sqlpipeAdminPassword); err != nil {
		log.Fatal(err)
	}

	data.ValidateUser(v, user)

	if !v.Valid() {
		log.Fatal(v.Errors)
	}

	etcd, err := clientv3.New(
		clientv3.Config{
			Endpoints:   etcdEndpoints,
			DialTimeout: time.Second * 5,
		},
	)
	if err != nil {
		log.Fatal(
			errors.New("unable to create an etcd client"),
		)
	}
	defer etcd.Close()

	session, err := concurrency.NewSession(etcd)
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	mutex := concurrency.NewMutex(session, "sqlpipe")
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	if err = mutex.Lock(ctx); err != nil {
		log.Fatal(err)
	}

	resp, err := etcd.Get(ctx, "sqlpipe", clientv3.WithPrefix())
	if err != nil {
		log.Fatal(err)
	}
	if resp.Count != 0 {
		log.Fatal("there is already a sqlpipe directory (or sub-keys) initialized in this etcd cluster. to initialize, you must delete the sqlpipe node, all children nodes, sqlpipe user, and sqlpipe role")
	}

	if _, err = etcd.Put(ctx, "sqlpipe", fmt.Sprint(time.Now())); err != nil {
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

	if err = m.Users.Insert(user); err != nil {
		log.Fatal(v.Errors)
	}

	if _, err = etcd.AuthEnable(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("initialized etcd cluster for usage with SQLpipe, and created a SQLpipe user called 'admin'")
}
