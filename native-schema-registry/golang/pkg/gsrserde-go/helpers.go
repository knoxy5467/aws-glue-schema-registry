package gsrserde

// Helper functions for GSR operations

// ValidateData validates input data
func ValidateData(data []byte) error {
	if data == nil || len(data) == 0 {
		return ErrInvalidData
	}
	return nil
}

// ValidateSchema validates schema definition
func ValidateSchema(schema string) error {
	if schema == "" {
		return ErrInvalidSchema
	}
	return nil
}
