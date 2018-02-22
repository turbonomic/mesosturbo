package conf

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"testing"
)

func TestEmptyConfig(t *testing.T) {
	conf := &MesosTargetConf{}
	bool, err := conf.validate()

	assert.Equal(t, false, bool, fmt.Sprintf("Validation should fail for empty config : %s", err))
}

func TestMissingMasterIPPort(t *testing.T) {
	conf := &MesosTargetConf{
		MasterUsername: "user",
		MasterPassword: "pwd",
	}
	bool, err := conf.validate()

	assert.Equal(t, false, bool, fmt.Sprintf("Validation should fail for empty MasterIPPort : %s", err))
}

func TestMissingMasterType(t *testing.T) {
	conf := &MesosTargetConf{
		MasterIPPort:   "127.0.0.1:5050",
		MasterUsername: "user",
		MasterPassword: "pwd",
	}
	bool, err := conf.validate()

	assert.Equal(t, false, bool, fmt.Sprintf("Validation not should fail : %s", err))

	fmt.Println("Framework : " + conf.Framework)
}

func TestGetAccountFields(t *testing.T) {
	conf := &MesosTargetConf{
		Master:         Apache,
		MasterIPPort:   "127.0.0.1:5050",
		MasterUsername: "user",
		MasterPassword: "pwd",
	}

	acctValues := conf.GetAccountValues()
	fmt.Println(acctValues)
	acctValuesMap := make(map[string]*proto.AccountValue)

	for _, acctVal := range acctValues {
		acctValuesMap[acctVal.GetKey()] = acctVal
	}

	checkAccountValueField(t, acctValuesMap[string(MasterIPPort)], string(MasterIPPort), conf.MasterIPPort)
	checkAccountValueField(t, acctValuesMap[string(MasterUsername)], string(MasterUsername), conf.MasterUsername)
	checkAccountValueField(t, acctValuesMap[string(MasterPassword)], string(MasterPassword), conf.MasterPassword)
}

func checkAccountValueField(t *testing.T, value *proto.AccountValue, propName string, propValue string) {
	assert.NotNil(t, value.GetKey())
	assert.NotNil(t, value.GetStringValue())
	assert.Equal(t, propValue, value.GetStringValue())
	assert.Equal(t, propName, value.GetKey())
}

func TestCreateMesosTargetConf(t *testing.T) {
	acctValues := createApacheAccValues()
	targetConf, err := CreateMesosTargetConf(string(Apache), acctValues)
	assert.Nil(t, err)
	_, ok := targetConf.(*MesosTargetConf)
	assert.True(t, true, ok)
}

func createApacheAccValues() []*proto.AccountValue {
	var accountValues []*proto.AccountValue
	prop1 := string(MasterIPPort)
	val1 := "10.10.10.10:5050"
	accVal := &proto.AccountValue{
		Key:         &prop1,
		StringValue: &val1,
	}
	accountValues = append(accountValues, accVal)

	prop3 := string(MasterUsername)
	val3 := "user"
	accVal = &proto.AccountValue{
		Key:         &prop3,
		StringValue: &val3,
	}
	accountValues = append(accountValues, accVal)

	prop4 := string(MasterPassword)
	val4 := "pwd"
	accVal = &proto.AccountValue{
		Key:         &prop4,
		StringValue: &val4,
	}
	accountValues = append(accountValues, accVal)
	return accountValues
}
