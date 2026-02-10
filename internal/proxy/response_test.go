package proxy

import (
	"testing"
)

// TestReadyForQueryTxStatus verifies that ReadyForQueryTxStatus returns the correct byte
// so that libpq/PDO see the correct transaction state (I=idle, T=in transaction).
func TestReadyForQueryTxStatus(t *testing.T) {
	p := &proxyConnection{}
	// No user transaction: must report idle so clients (e.g. PDO) do not think we're in a transaction.
	if got := p.ReadyForQueryTxStatus(); got != 'I' {
		t.Errorf("ReadyForQueryTxStatus() with 0 open transactions = %q, want 'I' (idle)", got)
	}

	// One BEGIN (user open count 1): must report in transaction so PDO sends COMMIT instead of throwing.
	p.IncrementUserOpenTransactionCount()
	if got := p.ReadyForQueryTxStatus(); got != 'T' {
		t.Errorf("ReadyForQueryTxStatus() with 1 open transaction = %q, want 'T' (in transaction)", got)
	}

	// Nested BEGIN (count 2): still in transaction.
	p.IncrementUserOpenTransactionCount()
	if got := p.ReadyForQueryTxStatus(); got != 'T' {
		t.Errorf("ReadyForQueryTxStatus() with 2 open transactions = %q, want 'T'", got)
	}

	// One COMMIT (count 1): still in transaction.
	_ = p.DecrementUserOpenTransactionCount()
	if got := p.ReadyForQueryTxStatus(); got != 'T' {
		t.Errorf("ReadyForQueryTxStatus() with 1 open transaction after decrement = %q, want 'T'", got)
	}

	// Outermost COMMIT (count 0): back to idle.
	_ = p.DecrementUserOpenTransactionCount()
	if got := p.ReadyForQueryTxStatus(); got != 'I' {
		t.Errorf("ReadyForQueryTxStatus() with 0 open transactions = %q, want 'I' (idle)", got)
	}
}
