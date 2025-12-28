package primitives

type AccountKind int

const (
	Asset AccountKind = iota
	Liability
	Equity
	Income
	Expense
)

type Account[K AccountKind] struct {
	Name    string
	Entries []*Entry
	Kind    K
}
