package discovery

import (
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

type EntityBuilder interface {
	BuildEntities() ([]*proto.EntityDTO, error)
}

var (
	DEFAULT_NAMESPACE string = "DEFAULT"
)

//TODO: change sdk builder to accept nil values
// Returns 0 if value is nil or not set
func getEntityMetricValue(mesosEntity MesosEntity, resourceType data.ResourceType, metricType data.MetricPropType, ec *ErrorCollector) *float64 {
	var zero_value = data.DEFAULT_VAL
	resourceMetric, err := mesosEntity.GetResourceMetric(resourceType, metricType)
	if err != nil {
		ec.Collect(err)
		return &zero_value
	}

	if resourceMetric.value != nil {
		return resourceMetric.value
	}

	return &zero_value
}
