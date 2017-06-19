package conf

// Represents the Mesos Master Vendor
type MesosMasterType string
const (

	Apache MesosMasterType = "Apache Mesos"
	DCOS   MesosMasterType = "Mesosphere DCOS"
)

// Represents the Framework used by the Mesos Master
type MesosFrameworkType string

const (
	Marathon      MesosFrameworkType = "Marathon"
	DCOS_Marathon MesosFrameworkType = "DCOS Marathon"
	Chronos       MesosFrameworkType = "Chronos"
	Hadoop        MesosFrameworkType = "Hadoop"
)

// ==========================================================================
type ProbeCategory string

const (
	CloudNative ProbeCategory = "Cloud Native"
)
// Constants for the Mesos probe account definition fields in the Turbo server
type ProbeAcctDefEntryName string

const (
	MasterIPPort   	ProbeAcctDefEntryName = "MasterIPPort"
	MasterUsername ProbeAcctDefEntryName = "Username"
	MasterPassword ProbeAcctDefEntryName = "Password"

	FrameworkIP       ProbeAcctDefEntryName = "FrameworkIP"
	FrameworkPort     ProbeAcctDefEntryName = "FrameworkPort"
	FrameworkUsername ProbeAcctDefEntryName = "FrameworkUsername"
	FrameworkPassword ProbeAcctDefEntryName = "FrameworkPassword"
)
