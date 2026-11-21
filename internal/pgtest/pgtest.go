// Package pgtest starts a throwaway PostgreSQL server for tests.
package pgtest

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// Start initialises and runs a PostgreSQL server in a temporary directory and
// returns a DSN for it. The test is skipped when PostgreSQL is not installed,
// or fails when X402_REQUIRE_TEST_SERVERS=1 (as in CI).
func Start(t testing.TB) string {
	t.Helper()
	skip := func(why string) {
		t.Helper()
		if os.Getenv("X402_REQUIRE_TEST_SERVERS") == "1" {
			t.Fatal(why + " and X402_REQUIRE_TEST_SERVERS=1")
		}
		t.Skip(why)
	}
	bins, _ := filepath.Glob("/usr/lib/postgresql/*/bin")
	binDir := ""
	if len(bins) > 0 {
		binDir = bins[len(bins)-1]
	} else if p, err := exec.LookPath("initdb"); err == nil {
		binDir = filepath.Dir(p)
	} else {
		skip("PostgreSQL server is not installed")
	}

	// PostgreSQL refuses to run as root; drop to the postgres user if we are.
	var cred *syscall.Credential
	dir, err := os.MkdirTemp("", "x402-pgtest-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if os.Getuid() == 0 {
		u, err := user.Lookup("postgres")
		if err != nil {
			skip("running as root and no postgres user exists")
		}
		uid, _ := strconv.Atoi(u.Uid)
		gid, _ := strconv.Atoi(u.Gid)
		cred = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
		if err := os.Chown(dir, uid, gid); err != nil {
			t.Fatal(err)
		}
	}
	run := func(name string, args ...string) *exec.Cmd {
		cmd := exec.Command(filepath.Join(binDir, name), args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
		cmd.Dir = dir
		return cmd
	}

	data := filepath.Join(dir, "data")
	if out, err := run("initdb", "-D", data, "-U", "x402", "-A", "trust", "--no-sync").CombinedOutput(); err != nil {
		t.Fatalf("initdb: %v\n%s", err, out)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	srv := run("postgres", "-D", data, "-p", fmt.Sprint(port), "-c", "listen_addresses=127.0.0.1",
		"-c", "unix_socket_directories=", "-c", "fsync=off")
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Process.Signal(syscall.SIGQUIT); srv.Wait() })

	deadline := time.Now().Add(15 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("postgres did not start")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Sprintf("postgres://x402@127.0.0.1:%d/postgres?sslmode=disable", port)
}
