package hammerdb

import "fmt"

// HammerDBConfig represents the HammerDB benchmark parameters
type HammerDBConfig struct {
	// Basic HammerDB settings
	Kind   string `yaml:"kind"`    // "pod" or "vm"
	DBType string `yaml:"db_type"` // "pg", "mariadb", "mssql"

	// Database initialization and benchmarking flags
	DBInit      bool `yaml:"db_init,omitempty"`      // Initialize database
	DBBenchmark bool `yaml:"db_benchmark,omitempty"` // Run benchmark

	// Job settings
	JobTimeout int `yaml:"job_timeout"` // Overall job timeout

	// Database connection settings
	DBServer   string `yaml:"db_server"`   // Database server hostname/IP
	DBPort     int    `yaml:"db_port"`     // Database port
	DBName     string `yaml:"db_name"`     // Database name
	DBUser     string `yaml:"db_user"`     // Database user
	DBPassword string `yaml:"db_password"` // Database password

	// TPC-C specific settings
	Warehouses   int `yaml:"warehouses"`    // Number of warehouses
	VirtualUsers int `yaml:"virtual_users"` // Number of virtual users
	RampupTime   int `yaml:"rampup_time"`   // Ramp-up time in minutes
	Duration     int `yaml:"duration"`      // Test duration in minutes

	// Container/VM settings
	Image        string `yaml:"image,omitempty"`         // HammerDB container image
	RuntimeClass string `yaml:"runtime_class,omitempty"` // Pod runtime class

	// VM settings (when kind=vm)
	VMImage  string `yaml:"vm_image,omitempty"`  // VM container image
	VMCores  int    `yaml:"vm_cores,omitempty"`  // VM CPU cores
	VMMemory string `yaml:"vm_memory,omitempty"` // VM memory
	VMBus    string `yaml:"vm_bus,omitempty"`    // VM disk bus type

	// Client VM PVC settings
	ClientVM ClientVMConfig `yaml:"client_vm,omitempty"`

	// Scheduling and placement
	Pin               bool              `yaml:"pin,omitempty"`      // Pin to specific node
	PinNode           string            `yaml:"pin_node,omitempty"` // Node to pin to
	NodeSelector      map[string]string `yaml:"nodeselector,omitempty"`
	Tolerations       interface{}       `yaml:"tolerations,omitempty"`
	Annotations       map[string]string `yaml:"annotations,omitempty"`
	ServerAnnotations map[string]string `yaml:"server_annotations,omitempty"`

	// Debug settings
	Debug bool `yaml:"debug,omitempty"` // Enable debug mode
}

// ClientVMConfig represents client VM PVC configuration
type ClientVMConfig struct {
	PVC             bool   `yaml:"pvc"`               // Enable PVC
	PVCStorageClass string `yaml:"pvc_storageclass"`  // Storage class for PVC
	PVCAccessMode   string `yaml:"pvc_pvcaccessmode"` // PVC access mode
	PVCVolumeMode   string `yaml:"pvc_pvcvolumemode"` // PVC volume mode
	PVCStorageSize  string `yaml:"pvc_storagesize"`   // PVC size
}

// SetDefaults sets default values for HammerDB configuration
func (h *HammerDBConfig) SetDefaults() {
	if h.Kind == "" {
		h.Kind = "pod"
	}

	if h.JobTimeout == 0 {
		h.JobTimeout = 3600
	}

	if h.Image == "" {
		h.Image = "quay.io/cloud-bulldozer/hammerdb:latest"
	}

	if h.VMImage == "" {
		h.VMImage = "quay.io/kubevirt/fedora-container-disk-images:latest"
	}

	if h.VMCores == 0 {
		h.VMCores = 2
	}

	if h.VMMemory == "" {
		h.VMMemory = "4G"
	}

	if h.VMBus == "" {
		h.VMBus = "virtio"
	}

	// Database-specific defaults
	switch h.DBType {
	case "pg":
		if h.DBPort == 0 {
			h.DBPort = 5432
		}
		if h.DBName == "" {
			h.DBName = "tpcc"
		}
		if h.DBUser == "" {
			h.DBUser = "postgres"
		}
	case "mariadb":
		if h.DBPort == 0 {
			h.DBPort = 3306
		}
		if h.DBName == "" {
			h.DBName = "tpcc"
		}
		if h.DBUser == "" {
			h.DBUser = "root"
		}
	case "mssql":
		if h.DBPort == 0 {
			h.DBPort = 1433
		}
		if h.DBName == "" {
			h.DBName = "tpcc"
		}
		if h.DBUser == "" {
			h.DBUser = "sa"
		}
	}

	// TPC-C defaults
	if h.Warehouses == 0 {
		h.Warehouses = 1
	}

	if h.VirtualUsers == 0 {
		h.VirtualUsers = 1
	}

	if h.RampupTime == 0 {
		h.RampupTime = 1
	}

	if h.Duration == 0 {
		h.Duration = 5
	}

	// Client VM defaults
	if h.ClientVM.PVCAccessMode == "" {
		h.ClientVM.PVCAccessMode = "ReadWriteOnce"
	}

	if h.ClientVM.PVCVolumeMode == "" {
		h.ClientVM.PVCVolumeMode = "Filesystem"
	}

	if h.ClientVM.PVCStorageSize == "" {
		h.ClientVM.PVCStorageSize = "5Gi"
	}
}

// Validate validates the HammerDB configuration
func (h *HammerDBConfig) Validate() error {
	if h.DBType == "" {
		return fmt.Errorf("db_type must be specified")
	}

	if h.DBType != "pg" && h.DBType != "mariadb" && h.DBType != "mssql" {
		return fmt.Errorf("db_type must be one of: pg, mariadb, mssql")
	}

	if h.DBServer == "" {
		return fmt.Errorf("db_server must be specified")
	}

	if h.Kind != "pod" && h.Kind != "vm" {
		return fmt.Errorf("kind must be either 'pod' or 'vm'")
	}

	if h.Warehouses <= 0 {
		return fmt.Errorf("warehouses must be greater than 0")
	}

	if h.VirtualUsers <= 0 {
		return fmt.Errorf("virtual_users must be greater than 0")
	}

	if !h.DBInit && !h.DBBenchmark {
		return fmt.Errorf("either db_init or db_benchmark (or both) must be enabled")
	}

	return nil
}
