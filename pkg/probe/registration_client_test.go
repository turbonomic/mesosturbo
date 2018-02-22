package probe

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"reflect"
	"testing"
)

func TestNewMesosRegistrationClient(t *testing.T) {
	var masters []conf.MesosMasterType
	masters = append(masters, conf.Apache)
	masters = append(masters, conf.DCOS)

	for _, y := range masters {
		expectedClient := &MesosRegistrationClient{
			mesosMasterType: y,
		}
		client := NewRegistrationClient(y)
		if !reflect.DeepEqual(expectedClient, client) {
			t.Errorf("\nExpected %+v, \ngot      %+v", expectedClient, client)
		}
	}
}

func TestMesosSupplyChain(t *testing.T) {

	client := NewRegistrationClient(conf.Apache)

	var supplychain []*proto.TemplateDTO
	supplychain = client.GetSupplyChainDefinition()

	// Entity Types
	var dtoMap map[proto.EntityDTO_EntityType]*proto.TemplateDTO
	dtoMap = make(map[proto.EntityDTO_EntityType]*proto.TemplateDTO)
	for _, templateDto := range supplychain {
		dtoMap[*templateDto.TemplateClass] = templateDto
	}

	assert.Contains(t, dtoMap, containerType, "Supply chain should contain Container")
	assert.Contains(t, dtoMap, vmType, "Supply chain should contain VM")
	assert.Contains(t, dtoMap, appType, "Supply chain should contain Application")
	assert.NotContains(t, dtoMap, proto.EntityDTO_APPLICATION_SERVER, "Should not contain ApplicationServer")

	containerDto := dtoMap[containerType]
	vmDto := dtoMap[vmType]
	appDto := dtoMap[appType]

	// External link
	assert.Equal(t, 1, len(containerDto.GetExternalLink()))
	assert.Equal(t, 0, len(vmDto.GetExternalLink()))
	assert.Equal(t, 0, len(appDto.GetExternalLink()))

	var links []*proto.TemplateDTO_ExternalEntityLinkProp
	links = containerDto.GetExternalLink()
	for _, prop := range links {
		link := prop.Value
		assert.Equal(t, vmType, link.GetSellerRef(), "Container Provider should be a VirtualMachine")
	}

	// Commodities sold
	expectedSoldComms := []proto.CommodityDTO_CommodityType{vCpuType, vMemType, appCommType}
	testCommsSold(t, containerDto, expectedSoldComms)

	expectedSoldComms = []proto.CommodityDTO_CommodityType{vCpuType, vMemType, vCpuProvisionedType, vMemProvisionedType, clusterType}
	testCommsSold(t, vmDto, expectedSoldComms)

	expectedSoldComms = []proto.CommodityDTO_CommodityType{}
	testCommsSold(t, appDto, expectedSoldComms)

	// Commodities Bought
	var expectedBoughtComms map[proto.EntityDTO_EntityType][]proto.CommodityDTO_CommodityType

	expectedBoughtComms = make(map[proto.EntityDTO_EntityType][]proto.CommodityDTO_CommodityType)
	expectedBoughtComms[vmType] = []proto.CommodityDTO_CommodityType{vCpuType, vMemType, vCpuProvisionedType, vMemProvisionedType, clusterType}
	testCommsBought(t, containerDto, expectedBoughtComms) // Container

	expectedBoughtComms = make(map[proto.EntityDTO_EntityType][]proto.CommodityDTO_CommodityType)
	expectedBoughtComms[containerType] = []proto.CommodityDTO_CommodityType{vCpuType, vMemType, appCommType}
	testCommsBought(t, appDto, expectedBoughtComms) // Application
}

func TestIdentifyingField(t *testing.T) {
	var masters []conf.MesosMasterType
	masters = append(masters, conf.Apache)
	masters = append(masters, conf.DCOS)

	idField := conf.MasterIPPort
	for _, masterType := range masters {
		client := NewRegistrationClient(masterType)
		assert.Equal(t, string(idField), client.GetIdentifyingFields())
	}
}

func TestIdentifyingFieldInMesosAcctMap(t *testing.T) {
	client := NewRegistrationClient(conf.Apache)

	idField := client.GetIdentifyingFields()
	var acctDefEntryMap map[string]*proto.AccountDefEntry
	acctDefEntryMap = make(map[string]*proto.AccountDefEntry)

	var accDefList []*proto.AccountDefEntry
	accDefList = client.GetAccountDefinition()
	for _, accDefEntry := range accDefList {
		acctDefEntryMap[*accDefEntry.GetCustomDefinition().Name] = accDefEntry
	}

	assert.Contains(t, acctDefEntryMap, idField)
}

func TestApacheMesosAccountMap(t *testing.T) {
	client := NewRegistrationClient(conf.Apache)

	expectedFields := [...]string{client.GetIdentifyingFields(), string(conf.MasterIPPort),
		string(conf.MasterUsername), string(conf.MasterPassword)}

	var acctDefEntryMap map[string]*proto.AccountDefEntry
	acctDefEntryMap = make(map[string]*proto.AccountDefEntry)

	var accDefList []*proto.AccountDefEntry
	accDefList = client.GetAccountDefinition()
	for _, accDefEntry := range accDefList {
		acctDefEntryMap[*accDefEntry.GetCustomDefinition().Name] = accDefEntry
	}

	for _, expectedField := range expectedFields {
		assert.Contains(t, acctDefEntryMap, expectedField)
	}
}

func TestDCOSMesosAccountMap(t *testing.T) {
	client := NewRegistrationClient(conf.DCOS)

	expectedFields := [...]string{client.GetIdentifyingFields(), string(conf.MasterIPPort),
		string(conf.MasterUsername), string(conf.MasterPassword)}
	absentFields := [...]string{string(conf.FrameworkIP), string(conf.FrameworkPort)}

	var acctDefEntryMap map[string]*proto.AccountDefEntry
	acctDefEntryMap = make(map[string]*proto.AccountDefEntry)

	var accDefList []*proto.AccountDefEntry
	accDefList = client.GetAccountDefinition()
	for _, accDefEntry := range accDefList {
		acctDefEntryMap[*accDefEntry.GetCustomDefinition().Name] = accDefEntry
	}

	for _, expectedField := range expectedFields {
		assert.Contains(t, acctDefEntryMap, expectedField)
	}

	for _, absentField := range absentFields {
		assert.NotContains(t, acctDefEntryMap, absentField)
	}
}

func testCommsSold(t *testing.T, templateDto *proto.TemplateDTO, expectedSoldComms []proto.CommodityDTO_CommodityType) {
	commsSold := templateDto.CommoditySold
	commsSoldMap := make(map[proto.CommodityDTO_CommodityType]*proto.TemplateCommodity)
	for _, commDto := range commsSold {
		commsSoldMap[*commDto.CommodityType] = commDto
	}
	for _, commtype := range expectedSoldComms {
		assert.Contains(t, commsSoldMap, commtype, fmt.Sprintf("template dto should sell commodity %s", commtype))

	}
}

func testCommsBought(t *testing.T, templateDto *proto.TemplateDTO, expectedBoughtComms map[proto.EntityDTO_EntityType][]proto.CommodityDTO_CommodityType) {
	var commsBought []*proto.TemplateDTO_CommBoughtProviderProp
	commsBought = templateDto.CommodityBought

	var templateCommsBought []*proto.TemplateCommodity
	for _, commBoughtProp := range commsBought {
		provider := commBoughtProp.Key
		templateCommsBought = commBoughtProp.Value
		testCommsBoughtPerProvider(t, templateCommsBought, expectedBoughtComms[*provider.TemplateClass])
	}
}

func testCommsBoughtPerProvider(t *testing.T, templateComms []*proto.TemplateCommodity, expectedBoughtComms []proto.CommodityDTO_CommodityType) {
	commsBoughtMap := make(map[proto.CommodityDTO_CommodityType]*proto.TemplateCommodity)
	for _, commDto := range templateComms {
		commsBoughtMap[*commDto.CommodityType] = commDto
	}
	for _, commtype := range expectedBoughtComms {
		assert.Contains(t, commsBoughtMap, commtype, fmt.Sprintf("template dto should buy commodity %s", commtype))

	}
}
