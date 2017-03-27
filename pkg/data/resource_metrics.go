package data

import (
	"github.com/golang/glog"
)

type MetricPropType string

const (
	USED    MetricPropType = "Used"
	CAP     MetricPropType = "Capacity"
	PEAK    MetricPropType = "Peak"
	AVERAGE MetricPropType = "Average"
)

type ResourceType string

const (
	CPU      ResourceType = "CPU"
	MEM      ResourceType = "MEM"
	DISK     ResourceType = "DISK"
	MEM_PROV ResourceType = "MEM_PROV"
	CPU_PROV ResourceType = "CPU_PROV"
)

type EntityType string

const (
	NODE EntityType = "Node"
	POD  EntityType = "Pod"
	APP  EntityType = "App"
)

type ResourceMetric interface {
	GetMetric() MetricPropType
	GetResourceType() ResourceType
	SetValue(data *float64)
	GetValue() *float64
}

// ====================== Entity Resource Metric ===========================

type EntityResourceMetrics struct {
	entityId                string
	resourceMetricMap       map[string]ResourceMetric
}

func NewEntityResourceMetrics(entityId string) *EntityResourceMetrics {
	return &EntityResourceMetrics{
		entityId:          entityId,
		resourceMetricMap: make(map[string]ResourceMetric),
	}
}

func (resourceMetrics *EntityResourceMetrics) SetResourceMetric(resource ResourceMetric) {
	mkey := string(resource.GetResourceType()) + "::" + string(resource.GetMetric())
	resourceMetrics.resourceMetricMap[mkey] = resource
}


func (resourceMetrics *EntityResourceMetrics) SetResourceMetricValue(resourceType ResourceType, metricType MetricPropType,
						value *float64) {
	var resource ResourceMetric
	mkey := string(resourceType) + "::" + string(metricType)
	resource, ok := resourceMetrics.resourceMetricMap[mkey]
	if !ok {
		resource = createResourceMetric(resourceType, metricType, value)
	}
	resourceMetrics.resourceMetricMap[mkey] = resource
}

func createResourceMetric(resourceType ResourceType, metricType MetricPropType, value *float64) ResourceMetric {
	var resource ResourceMetric
	if resourceType == CPU && metricType == CAP {
		resource = &CPUCapMetric{capacity: value,}
	} else if resourceType == CPU && metricType == USED {
		resource = &CPUUsedMetric{used: value,}
	} else if resourceType == MEM && metricType == CAP {
		resource = &MemCapMetric{capacity: value,}
	} else if resourceType == MEM && metricType == USED {
		resource = &MemUsedMetric{used: value,}
	} else if resourceType == CPU_PROV && metricType == CAP {
		resource = &CPUCProvCapMetric{capacity: value,}
	} else if resourceType == CPU_PROV && metricType == USED {
		resource = &CPUProvUsedMetric{used: value,}
	} else if resourceType == MEM_PROV && metricType == CAP {
		resource = &MemProvCapMetric{capacity: value,}
	} else if resourceType == MEM_PROV && metricType == USED {
		resource = &MemProvUsedMetric{used: value,}
	} else {
		glog.Errorf("Unknown resource : %s::%s", resourceType, metricType)
	}

	return resource
}

func (resourceMetrics *EntityResourceMetrics) GetResourceMetric(resourceType ResourceType, metricType MetricPropType) ResourceMetric {
	mkey := string(resourceType) + "::" + string(metricType)
	metric, ok := resourceMetrics.resourceMetricMap[mkey]
	if !ok {
		glog.V(4).Infof("Cannot find metrics for %s\n", mkey)
		return nil
	}
	return metric
}

func (resourceMetrics *EntityResourceMetrics) GetAllResourceMetrics() map[string]ResourceMetric {
	return resourceMetrics.resourceMetricMap
}

func (resourceMetrics *EntityResourceMetrics) printMetrics() {
	glog.Infof("======= Entity %s\n", resourceMetrics.entityId)
	for mkey, resourceMetric := range resourceMetrics.resourceMetricMap {
		if resourceMetric.GetValue() != nil {
			glog.Infof(" %s::%f\n", mkey, *resourceMetric.GetValue())
		}
	}
}

// ====================== Resource Metric ===========================
type CPUUsedMetric struct {
	used *float64
}

func NewCPUUsedMetric() *CPUUsedMetric {
	return &CPUUsedMetric{}
}

func (cpu *CPUUsedMetric) GetResourceType() ResourceType {
	return CPU
}

func (cpu *CPUUsedMetric) GetMetric() MetricPropType {
	return USED
}

func (cpu *CPUUsedMetric) SetValue(data *float64) {
	setUsedValueHelper(cpu.used, data)
	cpu.used = data
	if data != nil {
		glog.V(3).Infof("Set CPU Used %s %s\n", cpu.used, *cpu.used)
	}
}

func setUsedValueHelper(used *float64, data *float64) {
	if data != nil {
		glog.V(3).Infof("Setting to %s\n", used)
	} else {
		glog.V(3).Infof("Setting to nul data %s\n", used)
	}
}

func (cpu *CPUUsedMetric) GetValue() *float64 {
	return cpu.used
}

// ------------------------------
type CPUCapMetric struct {
	capacity *float64
}

func NewCPUCapMetric() *CPUCapMetric {
	return &CPUCapMetric{}
}

func (cpu *CPUCapMetric) GetResourceType() ResourceType {
	return CPU
}

func (cpu *CPUCapMetric) GetMetric() MetricPropType {
	return CAP
}

func (cpu *CPUCapMetric) SetValue(data *float64) {
	setCapValueHelper(cpu.capacity, data)
	cpu.capacity = data

	if data != nil {
		glog.V(3).Infof("Set CPU Capacity %s\n", cpu.capacity)
	}
}

func (cpu *CPUCapMetric) GetValue() *float64 {
	return cpu.capacity
}

func setCapValueHelper(cap *float64, data *float64) {
	if data != nil {
		glog.V(3).Infof("Setting to %s\n", cap)
	} else {
		glog.V(3).Infof("Setting to nul data  %s\n", cap)
	}
}

// ------------------------------
type CPUProvUsedMetric struct {
	used *float64
}

func NewCPUProvUsedMetric() *CPUProvUsedMetric {
	return &CPUProvUsedMetric{}
}

func (cpu *CPUProvUsedMetric) GetResourceType() ResourceType {
	return CPU_PROV
}

func (cpu *CPUProvUsedMetric) GetMetric() MetricPropType {
	return USED
}

func (cpu *CPUProvUsedMetric) SetValue(data *float64) {
	cpu.used = data
	setUsedValueHelper(cpu.used, data)
	if data != nil {
		glog.V(3).Infof("Set CPU Prov Used %s\n", cpu.used)
	}
}

func (cpu *CPUProvUsedMetric) GetValue() *float64 {
	return cpu.used
}

// ------------------------------
type CPUCProvCapMetric struct {
	capacity *float64
}

func NewCPUProvCapMetric() *CPUCProvCapMetric {
	return &CPUCProvCapMetric{}
}

func (cpu *CPUCProvCapMetric) GetResourceType() ResourceType {
	return CPU_PROV
}

func (cpu *CPUCProvCapMetric) GetMetric() MetricPropType {
	return CAP
}

func (cpu *CPUCProvCapMetric) SetValue(data *float64) {
	setCapValueHelper(cpu.capacity, data)
	cpu.capacity = data
	if data != nil {
		glog.V(3).Infof("Set CPU Prov Capacity %s\n", cpu.capacity)
	}
}

func (cpu *CPUCProvCapMetric) GetValue() *float64 {
	return cpu.capacity
}

// ------------------------------
type MemUsedMetric struct {
	used *float64
}

func NewMemUsedMetric() *MemUsedMetric {
	return &MemUsedMetric{}
}

func (mem *MemUsedMetric) GetResourceType() ResourceType {
	return MEM
}

func (mem *MemUsedMetric) GetMetric() MetricPropType {
	return USED
}

func (mem *MemUsedMetric) SetValue(data *float64) {
	mem.used = data
	setUsedValueHelper(mem.used, data)
	if data != nil {
		glog.V(3).Infof("Set Mem Used %s\n", mem.used)
	}
}

func (mem *MemUsedMetric) GetValue() *float64 {
	return mem.used
}

// ------------------------------
type MemCapMetric struct {
	capacity *float64
}

func NewMemCapMetric() *MemCapMetric {
	return &MemCapMetric{}
}

func (mem *MemCapMetric) GetResourceType() ResourceType {
	return MEM
}

func (mem *MemCapMetric) GetMetric() MetricPropType {
	return CAP
}

func (mem *MemCapMetric) SetValue(data *float64) {
	setCapValueHelper(mem.capacity, data)
	mem.capacity = data
	if data != nil {
		glog.V(3).Infof("Set Mem Capacity %s\n", mem.capacity)
	}
}

func (mem *MemCapMetric) GetValue() *float64 {
	return mem.capacity
}

// ------------------------------
type MemProvUsedMetric struct {
	used *float64
}

func NewMemProvUsedMetric() *MemProvUsedMetric {
	return &MemProvUsedMetric{}
}

func (mem *MemProvUsedMetric) GetResourceType() ResourceType {
	return MEM_PROV
}

func (mem *MemProvUsedMetric) GetMetric() MetricPropType {
	return USED
}

func (mem *MemProvUsedMetric) SetValue(data *float64) {
	setUsedValueHelper(mem.used, data)
	mem.used = data
	if data != nil {
		glog.V(3).Infof("Set Mem Prov Used %s\n", mem.used)
	}
}

func (mem *MemProvUsedMetric) GetValue() *float64 {
	return mem.used
}

// ------------------------------
type MemProvCapMetric struct {
	capacity *float64
}

func NewMemProvCapMetric() *MemProvCapMetric {
	return &MemProvCapMetric{}
}

func (mem *MemProvCapMetric) GetResourceType() ResourceType {
	return MEM_PROV
}

func (mem *MemProvCapMetric) GetMetric() MetricPropType {
	return CAP
}

func (mem *MemProvCapMetric) SetValue(data *float64) {
	setCapValueHelper(mem.capacity, data)
	mem.capacity = data
	if data != nil {
		glog.V(3).Infof("Set Mem Prov Capacity %s\n", mem.capacity)
	}
}

func (mem *MemProvCapMetric) GetValue() *float64 {
	return mem.capacity
}

// ------------------------------
type DiskUsedMetric struct {
	used *float64
}

func NewDiskUsedMetric() *DiskUsedMetric {
	return &DiskUsedMetric{}
}

func (disk *DiskUsedMetric) GetResourceType() ResourceType {
	return DISK
}

func (disk *DiskUsedMetric) GetMetric() MetricPropType {
	return USED
}

func (disk *DiskUsedMetric) SetValue(data *float64) {
	setUsedValueHelper(disk.used, data)
	disk.used = data
	if data != nil {
		glog.V(3).Infof("Set Disk Used %s\n", disk.used)
	}
}

func (disk *DiskUsedMetric) GetValue() *float64 {
	return disk.used
}

// ------------------------------
type DiskCapMetric struct {
	capacity *float64
}

func NewDiskCapMetric() *DiskCapMetric {
	return &DiskCapMetric{}
}

func (disk *DiskCapMetric) GetResourceType() ResourceType {
	return DISK
}

func (disk *DiskCapMetric) GetMetric() MetricPropType {
	return CAP
}

func (disk *DiskCapMetric) SetValue(data *float64) {
	setCapValueHelper(disk.capacity, data)
	disk.capacity = data
	if data != nil {
		glog.V(3).Infof("Set Disk Capacity %s\n", disk.capacity)
	}
}

func (disk *DiskCapMetric) GetValue() *float64 {
	return disk.capacity
}
