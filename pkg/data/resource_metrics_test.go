package data

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestNewCPUUsedMetric(t *testing.T) {
	cpuUsed := NewCPUUsedMetric()
	assert.Equal(t, CPU, cpuUsed.GetResourceType(), "Expected 'CPU'")
	assert.Equal(t, USED, cpuUsed.GetMetric(), "Expect 'USED'")

	cpuUsed.SetValue(nil)
	assert.Nil(t, cpuUsed.GetValue())

	var used float64
	used = 100.0
	cpuUsed.SetValue(&used)
	assert.Equal(t, 100.0, *cpuUsed.GetValue())

	used = 200.0
	cpuUsed.SetValue(&used)
	assert.Equal(t, 200.0, *cpuUsed.GetValue())

	cpuUsed.SetValue(nil)
	assert.Nil(t, cpuUsed.GetValue())
}


func TestNewCPUCapMetric(t *testing.T) {
	cpuCap := NewCPUCapMetric()
	assert.Equal(t, CPU, cpuCap.GetResourceType(), "Expected 'CPU'")
	assert.Equal(t, CAP, cpuCap.GetMetric(), "Expect 'CAP'")

	cpuCap.SetValue(nil)
	assert.Nil(t, cpuCap.GetValue())

	var cap float64
	cap = 100.0
	cpuCap.SetValue(&cap)
	assert.Equal(t, 100.0, *cpuCap.GetValue())

	cap = 200.0
	cpuCap.SetValue(&cap)
	assert.Equal(t, 200.0, *cpuCap.GetValue())
}

func TestNewMemUsedMetric(t *testing.T) {
	memUsed := NewMemUsedMetric()
	assert.Equal(t, MEM, memUsed.GetResourceType(), "Expected 'MEM'")
	assert.Equal(t, USED, memUsed.GetMetric(), "Expect 'USED'")

	memUsed.SetValue(nil)
	assert.Nil(t, memUsed.GetValue())

	var used float64
	used = 100.0
	memUsed.SetValue(&used)
	assert.Equal(t, 100.0, *memUsed.GetValue())

	used = 200.0
	memUsed.SetValue(&used)
	assert.Equal(t, 200.0, *memUsed.GetValue())

	memUsed.SetValue(nil)
	assert.Nil(t, memUsed.GetValue())
}


func TestNewMemCapMetric(t *testing.T) {
	memCap := NewMemCapMetric()
	assert.Equal(t, MEM, memCap.GetResourceType(), "Expected 'MEM'")
	assert.Equal(t, CAP, memCap.GetMetric(), "Expect 'CAP'")

	memCap.SetValue(nil)
	assert.Nil(t, memCap.GetValue())

	var cap float64
	cap = 100.0
	memCap.SetValue(&cap)
	assert.Equal(t, 100.0, *memCap.GetValue())

	cap = 200.0
	memCap.SetValue(&cap)
	assert.Equal(t, 200.0, *memCap.GetValue())
}


func TestNewCpuProvUsedMetric(t *testing.T) {
	memProvUsed := NewCPUProvUsedMetric()
	assert.Equal(t, CPU_PROV, memProvUsed.GetResourceType(), "Expected 'CPU_PROV'")
	assert.Equal(t, USED, memProvUsed.GetMetric(), "Expect 'USED'")

	memProvUsed.SetValue(nil)
	assert.Nil(t, memProvUsed.GetValue())

	var used float64
	used = 100.0
	memProvUsed.SetValue(&used)
	assert.Equal(t, 100.0, *memProvUsed.GetValue())

	used = 200.0
	memProvUsed.SetValue(&used)
	assert.Equal(t, 200.0, *memProvUsed.GetValue())

	memProvUsed.SetValue(nil)
	assert.Nil(t, memProvUsed.GetValue())
}


func TestNewCpuProvCapMetric(t *testing.T) {
	memProvCap := NewCPUProvCapMetric()
	assert.Equal(t, CPU_PROV, memProvCap.GetResourceType(), "Expected 'CPU_PROV'")
	assert.Equal(t, CAP, memProvCap.GetMetric(), "Expect 'CAP'")

	memProvCap.SetValue(nil)
	assert.Nil(t, memProvCap.GetValue())

	var cap float64
	cap = 100.0
	memProvCap.SetValue(&cap)
	assert.Equal(t, 100.0, *memProvCap.GetValue())

	cap = 200.0
	memProvCap.SetValue(&cap)
	assert.Equal(t, 200.0, *memProvCap.GetValue())
}

func TestNewMemProvUsedMetric(t *testing.T) {
	memProvUsed := NewMemProvUsedMetric()
	assert.Equal(t, MEM_PROV, memProvUsed.GetResourceType(), "Expected 'MEM_PROV'")
	assert.Equal(t, USED, memProvUsed.GetMetric(), "Expect 'USED'")

	memProvUsed.SetValue(nil)
	assert.Nil(t, memProvUsed.GetValue())

	var used float64
	used = 100.0
	memProvUsed.SetValue(&used)
	assert.Equal(t, 100.0, *memProvUsed.GetValue())

	used = 200.0
	memProvUsed.SetValue(&used)
	assert.Equal(t, 200.0, *memProvUsed.GetValue())

	memProvUsed.SetValue(nil)
	assert.Nil(t, memProvUsed.GetValue())
}


func TestNewMemProvCapMetric(t *testing.T) {
	memProvCap := NewMemProvCapMetric()
	assert.Equal(t, MEM_PROV, memProvCap.GetResourceType(), "Expected 'MEM_PROV'")
	assert.Equal(t, CAP, memProvCap.GetMetric(), "Expect 'CAP'")

	memProvCap.SetValue(nil)
	assert.Nil(t, memProvCap.GetValue())

	var cap float64
	cap = 100.0
	memProvCap.SetValue(&cap)
	assert.Equal(t, 100.0, *memProvCap.GetValue())

	cap = 200.0
	memProvCap.SetValue(&cap)
	assert.Equal(t, 200.0, *memProvCap.GetValue())
}

func TestNewNodeMetrics(t *testing.T) {
	nodeMetrics := NewEntityResourceMetrics("node1")
	assert.NotNil(t, nodeMetrics)
}

func TestSetNodeMetrics(t *testing.T) {
	nodeMetrics := NewEntityResourceMetrics("node1")
	assert.NotNil(t, nodeMetrics)

	var cpuCap float64
	cpuCap = 100.0

	nodeMetrics.SetResourceMetricValue(CPU, CAP, &cpuCap)

	cpuCapMetric2 := nodeMetrics.GetResourceMetric(CPU, CAP)
	assert.NotNil(t, cpuCapMetric2)

	cpuUsedMetric := nodeMetrics.GetResourceMetric(CPU, USED)
	assert.Nil(t, cpuUsedMetric)
}