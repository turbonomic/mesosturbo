package master

import (
	"fmt"
)

func ErrorCreateRequest(caller string, err error) error {
	return fmt.Errorf("["+caller+"] Error creating http request:\n %s", err)
}

func ErrorExecuteRequest(caller string, err error) error {
	return fmt.Errorf("["+caller+"] Error executing Mesos master request :\n %s", err)
}

func ErrorParseRequest(caller string, err error) error {
	return fmt.Errorf("["+caller+"] Error parsing Mesos master request :\n %s", err)
}

func ErrorConvertResponse(caller string, err error) error {
	return fmt.Errorf("["+caller+"]  Error converting Mesos master response :\n %s", err)
}

func ErrorEmptyResponse(caller string) error {
	return fmt.Errorf("[" + caller + "]  Response sent from mesos/DCOS master is nil")
}
