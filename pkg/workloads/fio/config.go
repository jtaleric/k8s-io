package fio

import "fmt"

// FIOConfig represents the FIO benchmark parameters
type FIOConfig struct {
	// Basic FIO settings
	Kind     string   `yaml:"kind"`     // "pod" or "vm"
	Servers  int      `yaml:"servers"`  // Number of FIO server pods/VMs
	Samples  int      `yaml:"samples"`  // Number of test iterations
	Jobs     []string `yaml:"jobs"`     // FIO job types (read, write, randread, etc.)
	BS       []string `yaml:"bs"`       // Block sizes
	BSRange  []string `yaml:"bsrange"`  // Block size ranges (alternative to bs)
	NumJobs  []int    `yaml:"numjobs"`  // Number of FIO processes per pod
	IODepth  int      `yaml:"iodepth"`  // Queue depth
	FileSize string   `yaml:"filesize"` // Size of files to test

	// Timing settings
	ReadRuntime   int `yaml:"read_runtime"`    // Read test duration
	WriteRuntime  int `yaml:"write_runtime"`   // Write test duration
	ReadRampTime  int `yaml:"read_ramp_time"`  // Read ramp-up time
	WriteRampTime int `yaml:"write_ramp_time"` // Write ramp-up time
	JobTimeout    int `yaml:"job_timeout"`     // Overall job timeout

	// Storage settings
	StorageClass  string `yaml:"storageclass,omitempty"`  // Kubernetes storage class
	StorageSize   string `yaml:"storagesize,omitempty"`   // PVC size
	PVCAccessMode string `yaml:"pvcaccessmode,omitempty"` // PVC access mode
	PVCVolumeMode string `yaml:"pvcvolumemode,omitempty"` // PVC volume mode
	HostPath      string `yaml:"hostpath,omitempty"`      // Host path for storage

	// Prefill settings
	Prefill          bool   `yaml:"prefill,omitempty"`            // Enable prefill
	PrefillBS        string `yaml:"prefill_bs,omitempty"`         // Prefill block size
	PostPrefillSleep int    `yaml:"post_prefill_sleep,omitempty"` // Sleep after prefill

	// VM settings (when kind=vm)
	VMImage  string `yaml:"vm_image,omitempty"`  // VM container image
	VMCores  int    `yaml:"vm_cores,omitempty"`  // VM CPU cores
	VMMemory string `yaml:"vm_memory,omitempty"` // VM memory
	VMBus    string `yaml:"vm_bus,omitempty"`    // VM disk bus type

	// Container settings
	Image        string `yaml:"image,omitempty"`         // FIO container image
	RuntimeClass string `yaml:"runtime_class,omitempty"` // Pod runtime class

	// Scheduling and placement
	NodeSelector      map[string]string `yaml:"nodeselector,omitempty"`
	Tolerations       interface{}       `yaml:"tolerations,omitempty"`
	Annotations       map[string]string `yaml:"annotations,omitempty"`
	ServerAnnotations map[string]string `yaml:"server_annotations,omitempty"`
	ClientAnnotations map[string]string `yaml:"client_annotations,omitempty"`

	// Logging and monitoring
	LogSampleRate int  `yaml:"log_sample_rate,omitempty"` // I/O stat sample interval
	LogHistMsec   int  `yaml:"log_hist_msec,omitempty"`   // Histogram logging interval
	FioJSONToLog  bool `yaml:"fio_json_to_log,omitempty"` // Log FIO JSON output
	Debug         bool `yaml:"debug,omitempty"`           // Enable debug mode

	// Compression settings
	CmpRatio int `yaml:"cmp_ratio,omitempty"` // Compression ratio

	// Cache drop settings
	DropCacheKernel   bool `yaml:"drop_cache_kernel,omitempty"`    // Drop kernel cache
	DropCacheRookCeph bool `yaml:"drop_cache_rook_ceph,omitempty"` // Drop Ceph cache
}

// JobParams represents job-specific parameters
type JobParams struct {
	JobnameMatch string   `yaml:"jobname_match"`
	Params       []string `yaml:"params"`
}

// SetDefaults sets default values for FIO configuration
func (f *FIOConfig) SetDefaults() {
	if f.Kind == "" {
		f.Kind = "pod"
	}

	if f.Servers == 0 {
		f.Servers = 1
	}

	if f.Samples == 0 {
		f.Samples = 1
	}

	if f.IODepth == 0 {
		f.IODepth = 4
	}

	if f.JobTimeout == 0 {
		f.JobTimeout = 3600
	}

	if f.Image == "" {
		f.Image = "quay.io/cloud-bulldozer/fio:latest"
	}

	if f.VMImage == "" {
		f.VMImage = "quay.io/kubevirt/fedora-container-disk-images:latest"
	}

	if f.VMCores == 0 {
		f.VMCores = 1
	}

	if f.VMMemory == "" {
		f.VMMemory = "5G"
	}

	if f.VMBus == "" {
		f.VMBus = "virtio"
	}

	if f.PVCAccessMode == "" {
		f.PVCAccessMode = "ReadWriteOnce"
	}

	if f.PVCVolumeMode == "" {
		f.PVCVolumeMode = "Filesystem"
	}

	if f.StorageSize == "" {
		f.StorageSize = "5Gi"
	}

	if f.PrefillBS == "" {
		f.PrefillBS = "4096KiB"
	}
}

// Validate validates the FIO configuration
func (f *FIOConfig) Validate() error {
	if len(f.Jobs) == 0 {
		return fmt.Errorf("at least one job type must be specified")
	}

	if len(f.BS) == 0 && len(f.BSRange) == 0 {
		return fmt.Errorf("either bs or bsrange must be specified")
	}

	if len(f.NumJobs) == 0 {
		return fmt.Errorf("at least one numjobs value must be specified")
	}

	if f.FileSize == "" {
		return fmt.Errorf("filesize must be specified")
	}

	if f.Kind != "pod" && f.Kind != "vm" {
		return fmt.Errorf("kind must be either 'pod' or 'vm'")
	}

	return nil
}

// GetFIOPath returns the FIO path based on storage configuration
func (f *FIOConfig) GetFIOPath() string {
	if f.StorageClass != "" {
		return "/dev/xvda"
	}
	return "/tmp"
}
