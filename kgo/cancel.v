// Cancel is franz-v's replacement for Go's context.Context: a shared
// cancellation signal with an optional deadline. Broker loops select on
// cancel.done alongside their request channels.
module kgo

import sync
import time

// Cancel carries a cancellation signal. Obtain one from new_cancel /
// new_cancel_timeout / child; the constructors are required because the
// done channel and shared identity must be initialized.
@[heap]
pub struct Cancel {
pub:
	// done fires (closes) when the Cancel is canceled; select on it.
	done chan bool
mut:
	mu          &sync.Mutex = sync.new_mutex()
	canceled    bool
	deadline_at i64 // unix microseconds; 0 = no deadline
}

// new_cancel returns a Cancel with no deadline.
pub fn new_cancel() &Cancel {
	return &Cancel{
		done: chan bool{}
	}
}

// new_cancel_timeout returns a Cancel that cancels itself after d.
pub fn new_cancel_timeout(d time.Duration) &Cancel {
	mut c := &Cancel{
		done:        chan bool{}
		deadline_at: time.now().unix_micro() + i64(d) / 1000
	}
	spawn fn (mut c Cancel, d time.Duration) {
		time.sleep(d)
		c.cancel()
	}(mut c, d)
	return c
}

// cancel cancels; it is safe to call more than once and from any thread.
pub fn (mut c Cancel) cancel() {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	if !c.canceled {
		c.canceled = true
		c.done.close()
	}
}

// is_done reports whether the Cancel has been canceled or its deadline has
// passed.
pub fn (mut c Cancel) is_done() bool {
	c.mu.lock()
	canceled := c.canceled
	deadline := c.deadline_at
	c.mu.unlock()
	if canceled {
		return true
	}
	return deadline != 0 && time.now().unix_micro() >= deadline
}

// remaining returns the time left until the deadline, or none if there is
// no deadline.
pub fn (mut c Cancel) remaining() ?time.Duration {
	c.mu.lock()
	deadline := c.deadline_at
	c.mu.unlock()
	if deadline == 0 {
		return none
	}
	left := deadline - time.now().unix_micro()
	if left < 0 {
		return time.Duration(0)
	}
	return time.Duration(left * 1000)
}

// child returns a Cancel that is canceled when either the parent is
// canceled or its own cancel is called.
pub fn (mut c Cancel) child() &Cancel {
	mut ch := &Cancel{
		done:        chan bool{}
		deadline_at: c.deadline_at
	}
	spawn fn (parent chan bool, mut ch Cancel) {
		select {
			_ := <-parent {
				ch.cancel()
			}
			_ := <-ch.done {
				// child canceled on its own; forwarder exits
			}
		}
	}(c.done, mut ch)
	return ch
}
