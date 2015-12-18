package main

import (
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/boltdb/bolt"
)

type EngineFormat int

const (
	b1 = iota
	bz1
	tsm1
)

// ShardInfo is the description of a shard on disk.
type ShardInfo struct {
	Database        string
	RetentionPolicy string
	Path            string
	Format          EngineFormat
	Size            int64
}

func (s *ShardInfo) FormatAsString() string {
	switch s.Format {
	case tsm1:
		return "tsm1"
	case b1:
		return "b1"
	case bz1:
		return "bz1"
	default:
		panic("unrecognized shard engine format")
	}
}

type ShardInfos []*ShardInfo

func (s ShardInfos) Len() int      { return len(s) }
func (s ShardInfos) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ShardInfos) Less(i, j int) bool {

	if s[i].Database == s[j].Database {
		if s[i].RetentionPolicy == s[j].RetentionPolicy {
			return s[i].Path < s[i].Path
		} else {
			return s[i].RetentionPolicy < s[j].RetentionPolicy
		}
	}
	return s[i].Database < s[j].Database
}

// Filter returns a copy of the shard infos, with shards of
// the given format removed.
func (s ShardInfos) Filter(fmt EngineFormat) ShardInfos {
	var a ShardInfos
	for _, ss := range s {
		if ss.Format == fmt {
			continue
		}
		a = append(a, ss)
	}
	return a
}

// Databases returns the unique set of databases for all shards
func (s ShardInfos) Databases() []string {
	dbm := make(map[string]bool)
	for _, ss := range s {
		dbm[ss.Database] = true
	}
	var dbk []string
	for k, _ := range dbm {
		dbk = append(dbk, k)
	}
	sort.Strings(dbk)
	return dbk
}

// Database represents an entire database on disk.
type Database struct {
	path string
}

// NewDatabase creates a database instance using data at path.
func NewDatabase(path string) *Database {
	return &Database{path: path}
}

// Name returns the name of the database.
func (d *Database) Name() string {
	return path.Base(d.path)
}

// Shards returns information for every shard in the database.
func (d *Database) Shards() ([]*ShardInfo, error) {
	fd, err := os.Open(d.path)
	if err != nil {
		return nil, err
	}

	// Get each retention policy.
	rps, err := fd.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	// Process each retention policy.
	var shardInfos []*ShardInfo
	for _, rp := range rps {
		rpfd, err := os.Open(filepath.Join(d.path, rp))
		if err != nil {
			return nil, err
		}

		// Process each shard
		shards, err := rpfd.Readdirnames(-1)
		for _, sh := range shards {
			fmt, sz, err := shardFormat(filepath.Join(d.path, rp, sh))
			if err != nil {
				return nil, err
			}

			si := &ShardInfo{
				Database:        d.Name(),
				RetentionPolicy: path.Base(rp),
				Path:            sh,
				Format:          fmt,
				Size:            sz,
			}
			shardInfos = append(shardInfos, si)
		}
	}

	sort.Sort(ShardInfos(shardInfos))
	return shardInfos, nil
}

// shardFormat returns the format and size on disk of the shard at path.
func shardFormat(path string) (EngineFormat, int64, error) {
	// If it's a directory then it's a tsm1 engine
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	fi, err := f.Stat()
	f.Close()
	if err != nil {
		return 0, 0, err
	}
	if fi.Mode().IsDir() {
		return tsm1, fi.Size(), nil
	}

	// It must be a BoltDB-based engine.
	db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()

	var format EngineFormat
	err = db.View(func(tx *bolt.Tx) error {
		// Retrieve the meta bucket.
		b := tx.Bucket([]byte("meta"))

		// If no format is specified then it must be an original b1 database.
		if b == nil {
			format = b1
			return nil
		}

		// "v1" engines are also b1.
		if string(b.Get([]byte("format"))) == "v1" {
			format = b1
			return nil
		}

		format = bz1
		return nil
	})
	return format, fi.Size(), err
}
