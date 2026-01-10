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
	Name     string
	Entries  SameCurrencyEntries
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

func (a *Account[K]) PostedBalance() Balance {
	var amount int64

	for _, entry := range a.Entries {
		if a.NormalBalance() == NormalDebit {
			if entry.Direction == Debit {
				amount += int64(entry.Value.Amount)
			} else {
				amount -= int64(entry.Value.Amount)
			}
		} else {
			if entry.Direction == Debit {
				amount -= int64(entry.Value.Amount)
			} else {
				amount += int64(entry.Value.Amount)
			}
		}
	}
	return Balance{
		Amount:   amount,
		Currency: a.Currency,
	}
}
