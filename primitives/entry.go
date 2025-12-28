package primitives

type EntryStatus int

const (
	Pending EntryStatus = iota
	Posted
	Archived
)

type Entry struct {
	direction string // credit/debit
	status    EntryStatus
	value     Money
}

// TODO ensure that entries in a BalancedEntries are balanced
type BalancedEntries []*Entry
