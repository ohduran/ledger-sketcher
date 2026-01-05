package primitives

type AccountKind int

const (
	Asset AccountKind = iota
	Liability
	Equity
	Income
	Expense
)

type AccountNormalBalance int

const (
	NormalDebit AccountNormalBalance = iota
	NormalCredit
)

type Account[K AccountKind] struct {
	Name    string
	Entries []*Entry

	// While Entries within a Transaction can have different Currencies, Entries within an Account
	// must have the same Currency.
	Currency Currency
	Kind     K
}

// In accounting, the normal balance of an account is the type of net balance that it should have.
func (a *Account[K]) NormalBalance() AccountNormalBalance {
	if AccountKind(a.Kind) == Asset || AccountKind(a.Kind) == Expense {
		return NormalDebit
	}
	return NormalCredit
}

// TODO
func (a *Account[K]) Balance() Money {
	var amount uint64

	return Money{
		Amount:   amount,
		Currency: a.Currency,
	}
}
