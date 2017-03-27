package discovery

import (
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

type EntityBuilder interface {

	BuildEntities() ([]*proto.EntityDTO, error)
}

var (
	DEFAULT_NAMESPACE string = "DEFAULT"
)


//TODO: change sdk builder to accept nil values
// Returns 0 if value is nil or not set
func getValue(nodeMetrics *data.EntityResourceMetrics, resourceType data.ResourceType, metricType data.MetricPropType) *float64 {
	var zero_value = 0.0
	resourceMetric := nodeMetrics.GetResourceMetric(resourceType, metricType)
	if resourceMetric == nil {
		return  &zero_value
	}

	if resourceMetric.GetValue() != nil {
		return resourceMetric.GetValue()
	}

	return &zero_value
}



