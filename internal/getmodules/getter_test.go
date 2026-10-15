// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package getmodules

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// testProxy records every request that reaches it.
var (
	testProxyMu       sync.Mutex
	testProxyHits     int
	testProxyLastHost string
)

// TestMain configures an HTTP proxy in the environment before any HTTP
// client is constructed. net/http's ProxyFromEnvironment snapshots the proxy
// environment only once per process, so the proxy has to be in place before
// the first request rather than set per-test.
//
// Requests to loopback addresses (like the httptest servers used by the other
// tests) bypass the proxy under Go's standard proxy rules, so only requests
// for non-loopback hosts are recorded.
func TestMain(m *testing.M) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testProxyMu.Lock()
		testProxyHits++
		testProxyLastHost = r.Host
		testProxyMu.Unlock()
		// Answer with a 404 rather than trying to forward anywhere; the tests
		// only care that the request was routed through the proxy.
		w.WriteHeader(http.StatusNotFound)
	}))
	defer proxy.Close()

	_ = os.Setenv("HTTP_PROXY", proxy.URL)
	_ = os.Setenv("http_proxy", proxy.URL)
	_ = os.Unsetenv("NO_PROXY")
	_ = os.Unsetenv("no_proxy")

	os.Exit(m.Run())
}

// testModuleArchive builds an in-memory .tar.gz module package containing a
// single module.txt marker file and returns the compressed bytes along with
// the expected file contents.
func testModuleArchive(t *testing.T, body string) []byte {
	t.Helper()

	pr, pw := io.Pipe()
	go func() {
		gz := gzip.NewWriter(pw)
		tw := tar.NewWriter(gz)
		hdr := &tar.Header{
			Name: "module.txt",
			Mode: 0o644,
			Size: int64(len(body)),
		}
		err := tw.WriteHeader(hdr)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = tw.Close()
		_ = gz.Close()
		_ = pw.Close()
	}()

	archive, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("failed to build test module archive: %s", err)
	}
	return archive
}

// newTestModuleServer returns an HTTP test server that serves a downloadable
// .tar.gz module package.
func newTestModuleServer(t *testing.T, archive []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// go-getter issues a HEAD request before the GET in its default
		// configuration; answer every method with the same archive.
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(archive)))
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = w.Write(archive)
	}))
}

// fetchAndVerify downloads the package at addr into a fresh directory and
// checks that the extracted marker file has the expected contents.
func fetchAndVerify(t *testing.T, fetcher *PackageFetcher, addr, wantBody string) {
	t.Helper()
	instDir := filepath.Join(t.TempDir(), "module")
	if err := fetcher.FetchPackage(context.Background(), instDir, addr); err != nil {
		t.Fatalf("unexpected error fetching %q: %s", addr, err)
	}
	got, err := os.ReadFile(filepath.Join(instDir, "module.txt"))
	if err != nil {
		t.Fatalf("failed to read extracted module file: %s", err)
	}
	if string(got) != wantBody {
		t.Fatalf("wrong module contents\nwant: %q\ngot:  %q", wantBody, string(got))
	}
}

// TestPackageFetcher_concurrentFetch ensures that a single PackageFetcher can
// be shared by several goroutines fetching packages concurrently without
// racing on its in-process reuse cache, and that a later fetch of the same
// package is served from the cache by copying.
func TestPackageFetcher_concurrentFetch(t *testing.T) {
	const body = "# concurrent module package\n"
	archive := testModuleArchive(t, body)
	server := newTestModuleServer(t, archive)
	defer server.Close()

	addr := server.URL + "/module.tar.gz"
	fetcher := NewPackageFetcher()

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			instDir := filepath.Join(t.TempDir(), "module")
			if err := fetcher.FetchPackage(context.Background(), instDir, addr); err != nil {
				errs <- err
				return
			}
			got, err := os.ReadFile(filepath.Join(instDir, "module.txt"))
			if err != nil {
				errs <- err
				return
			}
			if string(got) != body {
				errs <- fmt.Errorf("wrong module contents: %q", string(got))
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The package was fetched at least once above, so this final fetch must
	// be satisfied by copying the previously-installed directory instead of
	// making another network request.
	dir, ok := fetcher.getter.previousInstallDir(addr)
	if !ok {
		t.Fatalf("package %q was not recorded in the fetcher cache", addr)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("cached install directory %q is not available: %v", dir, err)
	}
	fetchAndVerify(t, fetcher, addr, body)
}

// TestReusingGetter_failedFetchIsNotCached ensures a download failure doesn't
// leave a stale entry in the reuse cache that would make a later attempt copy
// a partial or missing directory.
func TestReusingGetter_failedFetchIsNotCached(t *testing.T) {
	g := newReusingGetter()
	instDir := filepath.Join(t.TempDir(), "module")

	// "not-a-real-scheme" is a package address for which no getter is
	// configured, so go-getter must fail without touching the cache.
	err := g.getWithGoGetter(context.Background(), instDir, "not-a-real-scheme://example.com/package.tar.gz")
	if err == nil {
		t.Fatal("expected fetch of a package with an unsupported scheme to fail")
	}
	if dir, ok := g.previousInstallDir("not-a-real-scheme://example.com/package.tar.gz"); ok {
		t.Fatalf("failed fetch was unexpectedly cached at %q", dir)
	}
}

// TestReusingGetter_reusesPreviousInstall verifies the cache-hit branch: once
// a package has been installed, subsequent fetches copy from the remembered
// directory instead of downloading again.
func TestReusingGetter_reusesPreviousInstall(t *testing.T) {
	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "module.txt"), []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}

	g := newReusingGetter()
	g.rememberInstallDir("cached-package", srcDir)

	dst := filepath.Join(t.TempDir(), "copy")
	if err := g.getWithGoGetter(context.Background(), dst, "cached-package"); err != nil {
		t.Fatalf("unexpected error copying from cache: %s", err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "module.txt"))
	if err != nil {
		t.Fatalf("copied module file is missing: %s", err)
	}
	if string(got) != "cached" {
		t.Fatalf("wrong copied contents %q", string(got))
	}
}

// TestGetterHTTPClientHonorsProxyEnv verifies that the HTTP client used to
// download module packages routes requests for non-loopback hosts through the
// HTTP_PROXY proxy configured for the process in TestMain, rather than trying
// to resolve and connect to the download host directly.
func TestGetterHTTPClientHonorsProxyEnv(t *testing.T) {
	testProxyMu.Lock()
	before := testProxyHits
	testProxyMu.Unlock()

	// module-downloads.invalid does not exist in DNS, so this request can
	// only succeed at the transport level if it is sent to the test proxy.
	resp, err := getterHTTPClient.Get("http://module-downloads.invalid/package.tar.gz")
	if err != nil {
		t.Fatalf("request was not routed through the configured proxy: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected status from test proxy: %d", resp.StatusCode)
	}

	testProxyMu.Lock()
	defer testProxyMu.Unlock()
	if testProxyHits != before+1 {
		t.Fatalf("expected one request to reach the proxy, got %d", testProxyHits-before)
	}
	if testProxyLastHost != "module-downloads.invalid" {
		t.Fatalf("proxy was asked for %q, want the configured module download host", testProxyLastHost)
	}
}

// TestGetterHTTPClientBypassesProxyForLoopback verifies that downloads from
// local servers (the loopback addresses that back most private registries and
// mirrors) are not forced through the HTTP proxy, which is what lets the other
// tests in this package talk to their httptest servers with the proxy set.
func TestGetterHTTPClientBypassesProxyForLoopback(t *testing.T) {
	testProxyMu.Lock()
	before := testProxyHits
	testProxyMu.Unlock()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer origin.Close()

	resp, err := getterHTTPClient.Get(origin.URL)
	if err != nil {
		t.Fatalf("loopback request failed: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}

	testProxyMu.Lock()
	defer testProxyMu.Unlock()
	if testProxyHits != before {
		t.Fatalf("loopback request was unexpectedly sent through the proxy %d times", testProxyHits-before)
	}
}
