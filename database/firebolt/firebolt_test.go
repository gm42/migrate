package firebolt_test

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/dhui/dktest"
	_ "github.com/firebolt-db/firebolt-go-sdk"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/firebolt"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

const defaultPort = 3473

var (
	opts = dktest.Options{
		PortRequired: true,
		ReadyFunc:    isReady,
		ReadyTimeout: 10 * time.Second,
		PullTimeout:  5 * time.Minute,
		Timeout:      5 * time.Minute,
	}
	specs = []dktesting.ContainerSpec{
		{ImageName: "ghcr.io/firebolt-db/firebolt-core:preview-rc", Options: opts},
	}
)

func fireboltConnectionString(host, port string) string {
	return fmt.Sprintf("firebolt://?url=http://%s:%s", host, port)
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	ip, port, err := c.Port(defaultPort)
	if err != nil {
		return false
	}

	db, err := sql.Open("firebolt", fireboltConnectionString(ip, port))
	if err != nil {
		log.Println("open error:", err)
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	if err = db.PingContext(ctx); err != nil {
		switch err {
		case sqldriver.ErrBadConn:
			return false
		default:
			log.Println("ping error:", err)
		}
		return false
	}

	return true
}

func TestSimple(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fireboltConnectionString(ip, port) + "&x-multi-statement=true"
		p := &firebolt.Firebolt{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestWithInstance(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		conn, err := sql.Open("firebolt", fireboltConnectionString(ip, port))
		if err != nil {
			t.Fatal(err)
		}
		d, err := firebolt.WithInstance(conn, &firebolt.Config{})
		if err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fireboltConnectionString(ip, port) + "&x-multi-statement=true"
		p := &firebolt.Firebolt{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "firebolt", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}

func TestVersion(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fireboltConnectionString(ip, port) + "&x-multi-statement=true"
		p := &firebolt.Firebolt{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		expectedVersion := 1
		err = d.SetVersion(expectedVersion, false)
		if err != nil {
			t.Fatal(err)
		}

		version, _, err := d.Version()
		if err != nil {
			t.Fatal(err)
		}

		if version != expectedVersion {
			t.Fatal("Version mismatch")
		}
	})
}

func TestDrop(t *testing.T) {
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.Port(defaultPort)
		if err != nil {
			t.Fatal(err)
		}

		addr := fireboltConnectionString(ip, port) + "&x-multi-statement=true"
		p := &firebolt.Firebolt{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()

		err = d.Drop()
		if err != nil {
			t.Fatal(err)
		}
	})
}
