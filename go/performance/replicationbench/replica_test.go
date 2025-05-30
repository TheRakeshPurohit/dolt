// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverbench

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/gocraft/dbr/v2"
	"golang.org/x/sync/errgroup"

	srv "github.com/dolthub/dolt/go/cmd/dolt/commands/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils/testcommands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

type query string

type serverTest struct {
	name  string
	setup []query
	bench []query
}

// usage: `go test -bench .`
func BenchmarkPushOnWrite(b *testing.B) {

	setup := make([]query, 1)
	setup[0] = "CREATE TABLE bench (a int, b int, c int);"

	q := strings.Builder{}
	q.WriteString("INSERT INTO bench (a, b, c) VALUES (0, 0, 0)")
	i := 1
	for i < 1000 {
		q.WriteString(fmt.Sprintf(",(%d, %d, %d)", i, i, i))
		i++
	}
	qs := q.String()

	bench := make([]query, 100)
	commit := query("select dolt_commit('-am', 'cm')")
	i = 0
	for i < len(bench) {
		bench[i] = query(qs)
		bench[i+1] = commit
		i += 2
	}

	benchmarkServer(b, serverTest{
		name:  "smoke bench",
		setup: setup,
		bench: bench,
	})
}

func benchmarkServer(b *testing.B, test serverTest) {
	var dEnv *env.DoltEnv
	var cfg servercfg.ServerConfig
	ctx := context.Background()

	// setup
	dEnv, cfg = getEnvAndConfig(ctx, b)
	executeServerQueries(ctx, b, dEnv, cfg, test.setup)

	// bench
	f := getProfFile(b)
	err := pprof.StartCPUProfile(f)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		pprof.StopCPUProfile()
		if err = f.Close(); err != nil {
			b.Fatal(err)
		}
		fmt.Printf("\twriting CPU profile for %s: %s\n", b.Name(), f.Name())
	}()

	b.Run(test.name, func(b *testing.B) {
		executeServerQueries(ctx, b, dEnv, cfg, test.bench)
	})
}

const (
	database = "dolt_bench"
	port     = 1234

	name  = "name"
	email = "name@fake.horse"
)

func getEnvAndConfig(ctx context.Context, b *testing.B) (dEnv *env.DoltEnv, cfg servercfg.ServerConfig) {
	multiSetup := testcommands.NewMultiRepoTestSetup(b.Fatal)

	multiSetup.NewDB("dolt_bench")
	multiSetup.NewRemote("remote1")

	writerName := multiSetup.DbNames[0]

	localCfg, ok := multiSetup.GetEnv(writerName).Config.GetConfig(env.LocalConfig)
	if !ok {
		b.Fatal("local config does not exist")
	}
	localCfg.SetStrings(map[string]string{dsess.ReplicateToRemote: "remote1"})

	yaml := []byte(fmt.Sprintf(`
log_level: warning

behavior:
 read_only: false

user:
 name: "root"
 password: ""

databases:
 - name: "%s"
   path: "%s"

listener:
 host: localhost
 port: %d
 max_connections: 128
 read_timeout_millis: 28800000
 write_timeout_millis: 28800000
`, writerName, multiSetup.DbPaths[writerName], port))

	cfg, err := servercfg.NewYamlConfig(yaml)
	if err != nil {
		b.Fatal(err)
	}

	return multiSetup.GetEnv(writerName), cfg
}

func getProfFile(b *testing.B) *os.File {
	_, testFile, _, _ := runtime.Caller(0)

	f, err := os.Create(path.Join(path.Dir(testFile), b.Name()+".out"))
	if err != nil {
		b.Fatal(err)
	}
	return f
}

func executeServerQueries(ctx context.Context, b *testing.B, dEnv *env.DoltEnv, cfg servercfg.ServerConfig, queries []query) {
	sc := svcs.NewController()

	eg, ctx := errgroup.WithContext(ctx)

	//b.Logf("Starting server with Config %v\n", srv.ConfigInfo(cfg))
	eg.Go(func() (err error) {
		startErr, closeErr := srv.Serve(ctx, &srv.Config{
			Version:      "",
			ServerConfig: cfg,
			Controller:   sc,
			DoltEnv:      dEnv,
		})
		if startErr != nil {
			return startErr
		}
		if closeErr != nil {
			return closeErr
		}
		return nil
	})
	if err := sc.WaitForStart(); err != nil {
		b.Fatal(err)
	}

	for _, q := range queries {
		if err := executeQuery(cfg, q); err != nil {
			b.Fatal(err)
		}
	}

	sc.Stop()
	if err := sc.WaitForStop(); err != nil {
		b.Fatal(err)
	}
	if err := eg.Wait(); err != nil {
		b.Fatal(err)
	}
}

func executeQuery(cfg servercfg.ServerConfig, q query) error {
	cs := servercfg.ConnectionString(cfg, database)
	conn, err := dbr.Open("mysql", cs, nil)
	if err != nil {
		return err
	}

	rows, err := conn.Query(string(q))
	if err != nil {
		return err
	}

	for {
		if err = rows.Err(); err != nil {
			return err
		}
		if ok := rows.Next(); !ok {
			break
		}
	}

	return rows.Err()
}
