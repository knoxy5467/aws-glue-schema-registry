package gsrserde

import "errors"

// Common errors
var (
	ErrInvalidData   = errors.New("invalid data")
	ErrInvalidSchema = errors.New("invalid schema")
	ErrSerialization = errors.New("serialization error")
	ErrClosed        = errors.New("deserializer is closed")
	ErrNilData       = errors.New("data cannot be nil")
)
