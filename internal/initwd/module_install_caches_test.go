// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package initwd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	svchost "github.com/hashicorp/terraform-svchost"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs/configload"
	"github.com/hashicorp/terraform/internal/modsdir"
	"github.com/hashicorp/terraform/internal/registry/response"
)

// TestModuleInstaller_registryCachesConcurrent exercises the registry
// response caches of ModuleInstaller from several goroutines at once. When
// run under the race detector this guards the locks that make independent
// module packages safe to download concurrently.
func TestModuleInstaller_registryCachesConcurrent(t *testing.T) {
	inst := NewModuleInstaller(t.TempDir(), nil, nil, nil)

	host := svchost.Hostname("registry.example.com")

	const goroutines = 16
	const iterations = 50
	const packages = 32

	// Every goroutine writes the single value precomputed here for a given
	// key, so once a key has been populated cached reads can only ever return
	// that exact value.
	pkgAddrs := make([]addrs.ModuleRegistryPackage, packages)
	wantResps := make([]*response.ModuleVersions, packages)
	wantSources := make([]addrs.ModuleSourceRemote, packages)
	for idx := 0; idx < packages; idx++ {
		pkgAddrs[idx] = addrs.ModuleRegistryPackage{
			Host:         host,
			Namespace:    "namespace",
			Name:         fmt.Sprintf("module-%d", idx),
			TargetSystem: "generic",
		}
		wantResps[idx] = &response.ModuleVersions{}
		wantSources[idx] = addrs.ModuleSourceRemote{
			Package: addrs.ModulePackage(fmt.Sprintf("https://download.example.com/packages/%d.tar.gz", idx)),
		}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make(chan error, goroutines*iterations*2)
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				idx := (g + j) % packages
				pkg := pkgAddrs[idx]

				inst.rememberRegistryPackageVersions(pkg, wantResps[idx])
				gotResp, ok := inst.cachedRegistryPackageVersions(pkg)
				if !ok {
					errs <- fmt.Errorf("cached versions for %s disappeared", pkg)
					continue
				}
				if gotResp != wantResps[idx] {
					errs <- fmt.Errorf("unexpected versions response for %s", pkg)
				}

				ver := moduleVersion{module: pkg, version: "1.0.0"}
				inst.rememberRegistryPackageSource(ver, wantSources[idx])
				gotSource, ok := inst.cachedRegistryPackageSource(ver)
				if !ok {
					errs <- fmt.Errorf("cached download location for %s %s disappeared", pkg, ver.version)
					continue
				}
				if gotSource != wantSources[idx] {
					errs <- fmt.Errorf("unexpected download location for %s %s", pkg, ver.version)
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestModuleInstaller_registryCachesStartEmpty ensures the cache helpers
// report a miss before anything has been remembered and return exactly the
// value that was later recorded.
func TestModuleInstaller_registryCachesStartEmpty(t *testing.T) {
	inst := NewModuleInstaller(t.TempDir(), nil, nil, nil)

	pkg := addrs.ModuleRegistryPackage{
		Host:         svchost.Hostname("registry.example.com"),
		Namespace:    "namespace",
		Name:         "module",
		TargetSystem: "generic",
	}
	ver := moduleVersion{module: pkg, version: "2.0.0"}

	if _, ok := inst.cachedRegistryPackageVersions(pkg); ok {
		t.Fatal("expected no cached versions before the registry was consulted")
	}
	if _, ok := inst.cachedRegistryPackageSource(ver); ok {
		t.Fatal("expected no cached download location before the registry was consulted")
	}

	wantResp := &response.ModuleVersions{}
	inst.rememberRegistryPackageVersions(pkg, wantResp)
	if gotResp, ok := inst.cachedRegistryPackageVersions(pkg); !ok || gotResp != wantResp {
		t.Fatal("the remembered versions response was not returned from the cache")
	}

	wantSource := addrs.ModuleSourceRemote{Package: addrs.ModulePackage("https://example.com/package.tar.gz")}
	inst.rememberRegistryPackageSource(ver, wantSource)
	if gotSource, ok := inst.cachedRegistryPackageSource(ver); !ok || gotSource != wantSource {
		t.Fatal("the remembered download location was not returned from the cache")
	}
}

// TestModuleInstaller_failedInstallDoesNotWriteManifest verifies that when
// module installation fails, the error is reported without a partial manifest
// being written to disk, so the modules.json cache can never end up recording
// modules that were never successfully installed.
func TestModuleInstaller_failedInstallDoesNotWriteManifest(t *testing.T) {
	fixtureDir := filepath.Clean("testdata/local-module-error")
	dir, done := tempChdir(t, fixtureDir)
	defer done()

	loader, closeLoader := configload.NewLoaderForTests(t)
	defer closeLoader()

	modulesDir := filepath.Join(dir, ".terraform", "modules")
	inst := NewModuleInstaller(modulesDir, loader, nil, testModuleInstallerInitializer(loader))
	_, diags := inst.InstallModules(context.Background(), ".", "tests", false, false)
	if !diags.HasErrors() {
		t.Fatal("expected module installation to fail for the error fixture")
	}

	manifestPath := filepath.Join(modulesDir, modsdir.ManifestSnapshotFilename)
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		if err == nil {
			t.Fatalf("a failed module installation must not write the manifest, but %s exists", manifestPath)
		}
		t.Fatalf("unexpected error checking for %s: %s", manifestPath, err)
	}
}
