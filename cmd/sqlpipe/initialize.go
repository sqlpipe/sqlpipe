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
	"github.com/sqlpipe/sqlpipe/internal/globals"
)

var (
	InitializeCmd = &cobra.Command{
		Use:   "initialize",
		Short: "Prepare an etcd cluster for usage by SQLpipe.",
		Long:  "Prepare an etcd cluster for usage by SQLpipe. Must have authentication disabled. SQLpipe will create a user, role, and directory called sqlpipe, and grant the user access to the role, and the role access to that directory. It will also give the root user a password, and enable authentication.",
		Run:   initializeCmd,
	}
	rootPassword    string
	sqlpipePassword string
	etcdEndpoints   []string
)

func init() {
	InitializeCmd.Flags().StringVar(&rootPassword, "root-password", "", "etcd root password")
	InitializeCmd.Flags().StringVar(&sqlpipePassword, "sqlpipe-password", "", "password for new 'sqlpipe' user")
	InitializeCmd.Flags().StringSliceVar(&etcdEndpoints, "etcd-endpoints", []string{}, "etcd endpoints, comma separated no spaces")
}

func initializeCmd(cmd *cobra.Command, args []string) {
	if reflect.DeepEqual(etcdEndpoints, []string{}) {
		log.Fatal(errors.New("--etcd-cluster flag given without specifying cluster endpoints"))
	}

	if rootPassword == "" {
		log.Fatal("--root-password empty")
	}

	if sqlpipePassword == "" {
		log.Fatal("--sqlpipe-password empty")
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
	if _, err = etcd.UserAdd(ctx, "root", rootPassword); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserGrantRole(ctx, "root", "root"); err != nil {
		log.Fatal(err)
	}

	if _, err = etcd.RoleAdd(ctx, "sqlpipe"); err != nil {
		log.Fatal(err)
	}
	if _, err = etcd.UserAdd(ctx, "sqlpipe", sqlpipePassword); err != nil {
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

	if _, err = etcd.AuthEnable(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("initialized etcd cluster for usage with SQLpipe")
}
