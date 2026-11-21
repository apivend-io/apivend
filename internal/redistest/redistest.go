// Package redistest starts a throwaway redis-server for tests.
package redistest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Start runs redis-server on a free port and returns a client and its
// address. The test is skipped when the binary is not installed, or fails
// when X402_REQUIRE_TEST_SERVERS=1 (as in CI, where a skip would hide that
// the money-path tests did not run).
func Start(t testing.TB) (*redis.Client, string) {
	t.Helper()
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("X402_REQUIRE_TEST_SERVERS") == "1" {
			t.Fatal("redis-server not installed and X402_REQUIRE_TEST_SERVERS=1")
		}
		t.Skip("redis-server not installed")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--bind", "127.0.0.1",
		"--save", "", "--appendonly", "no", "--dir", t.TempDir())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for rdb.Ping(context.Background()).Err() != nil {
		if time.Now().After(deadline) {
			t.Fatal("redis-server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return rdb, addr
}
