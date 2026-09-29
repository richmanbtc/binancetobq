package watchdog

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestWatchdogPingResetsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fired := make(chan struct{}, 1)
		w := New(20*time.Minute, func() { fired <- struct{}{} })
		defer w.Close()
		// The first upload uses the same limit as every subsequent upload.
		time.Sleep(20*time.Minute - time.Second)
		synctest.Wait()
		if len(fired) != 0 {
			t.Fatal("save deadline expired early")
		}
		w.Ping()
		time.Sleep(20*time.Minute - time.Second)
		synctest.Wait()
		if len(fired) != 0 {
			t.Fatal("successful save failed to reset deadline")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(fired) != 1 {
			t.Fatal("missing saves did not trigger exit")
		}
	})
}

func TestFirstSaveHasNoExtraGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fired := make(chan struct{}, 1)
		w := New(20*time.Minute, func() { fired <- struct{}{} })
		defer w.Close()
		time.Sleep(20 * time.Minute)
		synctest.Wait()
		if len(fired) != 1 {
			t.Fatal("first save received extra startup grace")
		}
	})
}

func TestWatchdogTimeoutUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fired := make(chan struct{}, 1)
		w := New(time.Minute, func() { fired <- struct{}{} })
		defer w.Close()
		time.Sleep(2 * time.Second)
		w.SetTimeout(3 * time.Second)
		time.Sleep(time.Second)
		synctest.Wait()
		if len(fired) != 1 {
			t.Fatal("timeout update reset or failed to shorten deadline")
		}
	})
}
