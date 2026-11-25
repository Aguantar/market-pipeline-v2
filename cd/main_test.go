package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestShort(t *testing.T) {
	if got := short("sha256:d63b25dbe90db013767947c9b2509680267867b4"); got != "d63b25dbe90d" {
		t.Fatalf("short = %q", got)
	}
}

func TestLoadCfgAndState(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "cd.env")
	os.WriteFile(p, []byte("MODE=deploy\nIMAGE=ghcr.io/aguantar/x\nCOMPOSE_FILE=/tmp/c.yml\nSERVICES=a,b\nSTATE="+filepath.Join(dir, "s.json")+"\nVERIFY_SEC=5\n"), 0o600)
	c, err := loadCfg(p)
	if err != nil || c.Mode != "deploy" || len(c.Services) != 2 || c.VerifySec != 5 || c.Tag != "latest" {
		t.Fatalf("cfg %+v err %v", c, err)
	}
	st := loadState(c.State)
	st.LastDigest = "sha256:abc"
	if err := saveState(c.State, st); err != nil {
		t.Fatal(err)
	}
	if loadState(c.State).LastDigest != "sha256:abc" {
		t.Fatal("state roundtrip")
	}
	if _, err := loadCfg(filepath.Join(dir, "missing.env")); err == nil {
		t.Fatal("missing cfg should fail")
	}
}
