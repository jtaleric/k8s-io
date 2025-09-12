package config

import (
	"fmt"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the main benchmark configuration
type Config struct {
	// Kubernetes settings
	Namespace string `yaml:"namespace"`

	// Benchmark identification
	UUID        string `yaml:"uuid,omitempty"`
	TestUser    string `yaml:"test_user"`
	ClusterName string `yaml:"clustername"`

	// Workload selection
	Workload WorkloadConfig `yaml:"workload"`

	// Elasticsearch configuration (optional)
	Elasticsearch *ElasticsearchConfig `yaml:"elasticsearch,omitempty"`

	// Prometheus configuration (optional)
	Prometheus *PrometheusConfig `yaml:"prometheus,omitempty"`

	// Cache drop settings
	KCacheDropPodIPs       string `yaml:"kcache_drop_pod_ips,omitempty"`
	KernelCacheDropSvcPort int    `yaml:"kernel_cache_drop_svc_port,omitempty"`
	CephOSDCacheDropPodIP  string `yaml:"ceph_osd_cache_drop_pod_ip,omitempty"`
	CephCacheDropSvcPort   int    `yaml:"ceph_cache_drop_svc_port,omitempty"`
	RookCephDropCachePodIP string `yaml:"rook_ceph_drop_cache_pod_ip,omitempty"`

	// FIO job parameters
	JobParams []JobParam `yaml:"job_params,omitempty"`
}

// WorkloadConfig represents the workload selection and configuration
type WorkloadConfig struct {
	Name string      `yaml:"name"` // "fio" or "hammerdb"
	Args interface{} `yaml:"args"` // Will be unmarshaled to specific workload config
}

// ElasticsearchConfig represents Elasticsearch settings
type ElasticsearchConfig struct {
	URL        string `yaml:"url"`
	IndexName  string `yaml:"index_name,omitempty"`
	VerifyCert bool   `yaml:"verify_cert,omitempty"`
	Parallel   bool   `yaml:"parallel,omitempty"`
}

// PrometheusConfig represents Prometheus settings
type PrometheusConfig struct {
	ESURL      string `yaml:"es_url,omitempty"`
	ESParallel bool   `yaml:"es_parallel,omitempty"`
	PromToken  string `yaml:"prom_token,omitempty"`
	PromURL    string `yaml:"prom_url,omitempty"`
}

// JobParam represents FIO job parameters
type JobParam struct {
	JobnameMatch string   `yaml:"jobname_match"`
	Params       []string `yaml:"params"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filename, err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", filename, err)
	}

	// Set defaults
	config.setDefaults()

	// Validate configuration
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// setDefaults sets default values for configuration
func (c *Config) setDefaults() {
	if c.TestUser == "" {
		c.TestUser = "ripsaw"
	}

	if c.ClusterName == "" {
		c.ClusterName = "default-cluster"
	}

	if c.Namespace == "" {
		c.Namespace = "default"
	}

	// Generate UUID if not provided
	if c.UUID == "" {
		c.UUID = generateUUID()
	}
}

// validate validates the configuration
func (c *Config) validate() error {
	if c.Workload.Name == "" {
		return fmt.Errorf("workload name must be specified")
	}

	if c.Workload.Name != "fio" && c.Workload.Name != "hammerdb" {
		return fmt.Errorf("workload name must be either 'fio' or 'hammerdb'")
	}

	return nil
}

// GetTruncatedUUID returns the first 8 characters of the UUID
func (c *Config) GetTruncatedUUID() string {
	if len(c.UUID) >= 8 {
		return c.UUID[:8]
	}
	return c.UUID
}

// generateUUID generates a simple UUID-like string
func generateUUID() string {
	// Simple UUID generation - in production, use a proper UUID library
	return fmt.Sprintf("%d", time.Now().UnixNano())[:8]
}
