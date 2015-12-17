package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

const description = `
Convert a database shards from b1 or bz1 format to tsm1 format.

This tool will backup all databases before conversion occurs. It
is up to the end-user to delete the backup on the disk. Backups are
named by suffixing the database name with '.bak'. The backups will
be ignored by the system since they are not registered with the cluster.

To restore a backup, delete the tsm version, rename the backup and
restart the node.`

var dbs string

func init() {
	flag.StringVar(&dbs, "dbs", "", "Comma-delimited list of databases to convert. Default is convert all")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <data-path> \n", os.Args[0])
		fmt.Fprintf(os.Stderr, "%s\n\n", description)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "no data directory specified\n")
		os.Exit(1)
	}
	dataPath := os.Args[1]

	// Dump the list of convertible shards.
	dbs, err := ioutil.ReadDir(dataPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to access data directory at %s: %s\n", dataPath, err.Error())
		os.Exit(1)
	}

	// Get the list of shards for conversion.
	var shards []*ShardInfo
	for _, db := range dbs {
		d := NewDatabase(filepath.Join(dataPath, db.Name()))
		shs, err := d.Shards()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to access shards for database %s: %s\n", d.Name(), err.Error())
			os.Exit(1)
		}
		shards = append(shards, shs...)
	}
	sort.Sort(ShardInfos(shards))
	shards = ShardInfos(shards).Filter(tsm1)

	for i, si := range shards {
		fmt.Printf("%d: %v\n", i, si)
	}
}
