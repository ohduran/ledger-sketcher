package primitives

type Currency struct {
	Code     string // ISO 4217 Currency Code
	Exponent uint64 // The number of digits after the decimal separator
}

type Money[T int64 | uint64] struct {
	Amount   T
	Currency Currency
}

// Type aliases for clarity and intent
type Balance = Money[int64]                  // Can be negative (used for account balances)
type MoneyWithPositiveAmount = Money[uint64] // Must be positive (used for Entry values)
