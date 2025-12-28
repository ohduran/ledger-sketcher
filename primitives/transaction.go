package primitives

type Transaction struct {
	Entries BalancedEntries
	status  EntryStatus
}

func NewTransaction(entries BalancedEntries) *Transaction {
	return &Transaction{
		Entries: entries,
		status:  Pending,
	}
}
