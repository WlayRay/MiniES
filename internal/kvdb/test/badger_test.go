package kvdbtest

import (
	"testing"

	"github.com/WlayRay/ElectricSearch/config"
	"github.com/WlayRay/ElectricSearch/internal/kvdb"
)

func TestBadger(t *testing.T) {
	setup = func() {
		var err error
		db, err = kvdb.GetKeyValueDB(kvdb.BADGER, config.RootPath+"data/badger_db")
		if err != nil {
			panic(err)
		}
	}

	t.Run("badger_test", testPipeline)
}
