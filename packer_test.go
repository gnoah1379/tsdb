package tsdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPacker_packKey(t *testing.T) {
	p := packerPool.Get()
	measurement := "cpu"
	timestamp := time.Now()
	labels := map[string]string{"host": "server1", "region": "us-west"}
	result := p.packKey(measurement, timestamp, labels)
	measurementRet, tsRet, labelsRet, err := p.unpackKey(result)
	assert.NoError(t, err)
	assert.Equal(t, measurement, measurementRet)
	assert.Equal(t, timestamp.UnixNano(), tsRet.UnixNano())
	assert.Equal(t, labels, labelsRet)
}

func TestPacker_packFields(t *testing.T) {
	p := packerPool.Get()
	fields := map[string]float64{"usage": 0.5, "idle": 0.5}

	result := p.packFields(fields)
	resultFields, err := p.unpackFields(result)
	assert.NoError(t, err)
	assert.Equal(t, fields, resultFields)
}
