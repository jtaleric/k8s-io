package workloads

import (
	"context"
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
	"github.com/jtaleric/k8s-io/pkg/workloads/fio"
	"github.com/jtaleric/k8s-io/pkg/workloads/hammerdb"
)

// Workload represents a benchmark workload
type Workload interface {
	// GetName returns the workload name
	GetName() string

	// Validate validates the workload configuration
	Validate() error

	// GenerateManifests generates all Kubernetes manifests for the workload
	GenerateManifests() (map[string]string, error)

	// RunBenchmark executes the complete benchmark
	RunBenchmark(ctx context.Context) error

	// Cleanup removes all resources created by the benchmark
	Cleanup(ctx context.Context) error
}

// Factory creates workloads based on configuration
type Factory struct {
	k8sClient *kubernetes.Client
	config    *config.Config
}

// NewFactory creates a new workload factory
func NewFactory(k8sClient *kubernetes.Client, cfg *config.Config) *Factory {
	return &Factory{
		k8sClient: k8sClient,
		config:    cfg,
	}
}

// CreateWorkload creates a workload based on the configuration
func (f *Factory) CreateWorkload() (Workload, error) {
	switch f.config.Workload.Name {
	case "fio":
		return f.createFIOWorkload()
	case "hammerdb":
		return f.createHammerDBWorkload()
	default:
		return nil, fmt.Errorf("unsupported workload: %s", f.config.Workload.Name)
	}
}

// createFIOWorkload creates a FIO workload
func (f *Factory) createFIOWorkload() (Workload, error) {
	// Marshal the args back to YAML and unmarshal to FIOConfig
	argsData, err := yaml.Marshal(f.config.Workload.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal FIO args: %w", err)
	}

	var fioConfig fio.FIOConfig
	if err := yaml.Unmarshal(argsData, &fioConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal FIO config: %w", err)
	}

	// Set defaults and validate
	fioConfig.SetDefaults()
	if err := fioConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid FIO configuration: %w", err)
	}

	return fio.NewWorkload(f.k8sClient, f.config, &fioConfig)
}

// createHammerDBWorkload creates a HammerDB workload
func (f *Factory) createHammerDBWorkload() (Workload, error) {
	// Marshal the args back to YAML and unmarshal to HammerDBConfig
	argsData, err := yaml.Marshal(f.config.Workload.Args)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal HammerDB args: %w", err)
	}

	var hammerdbConfig hammerdb.HammerDBConfig
	if err := yaml.Unmarshal(argsData, &hammerdbConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal HammerDB config: %w", err)
	}

	// Set defaults and validate
	hammerdbConfig.SetDefaults()
	if err := hammerdbConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid HammerDB configuration: %w", err)
	}

	return hammerdb.NewWorkload(f.k8sClient, f.config, &hammerdbConfig)
}
