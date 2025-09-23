package gsrserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfigFromMap_Basic(t *testing.T) {
	config, err := LoadConfigFromMap(map[string]string{})
	assert.NoError(t, err)
	assert.NotNil(t, config)
}
