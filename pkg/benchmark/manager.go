package benchmark

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
	"github.com/jtaleric/k8s-io/pkg/templates"
)

// State represents the benchmark execution state
type State string

const (
	StateBuilding        State = "Building"
	StateStartingServers State = "StartingServers"
	StateStartingClient  State = "StartingClient"
	StatePrefilling      State = "Prefilling"
	StateStartBenchmark  State = "StartBenchmark"
	StateRunning         State = "Running"
	StateCompleted       State = "Completed"
	StateFailed          State = "Failed"
)

// Manager manages the FIO distributed benchmark execution
type Manager struct {
	k8sClient      *kubernetes.Client
	templateEngine *templates.Engine
	config         *config.Config
	state          State
	podDetails     map[string]string
}

// NewManager creates a new benchmark manager
func NewManager(k8sClient *kubernetes.Client, templateEngine *templates.Engine, cfg *config.Config) *Manager {
	return &Manager{
		k8sClient:      k8sClient,
		templateEngine: templateEngine,
		config:         cfg,
		state:          StateBuilding,
		podDetails:     make(map[string]string),
	}
}

// GenerateManifests generates all Kubernetes manifests
func (m *Manager) GenerateManifests() (map[string]string, error) {
	manifests := make(map[string]string)

	// Generate FIO test configmap
	configMap, err := m.templateEngine.RenderConfigMap(m.config)
	if err != nil {
		return nil, fmt.Errorf("failed to render configmap: %w", err)
	}
	manifests["fio-configmap"] = configMap

	// Generate prefill configmap if prefill is enabled
	if m.config.WorkloadArgs.Prefill {
		prefillConfigMap, err := m.templateEngine.RenderPrefillConfigMap(m.config)
		if err != nil {
			return nil, fmt.Errorf("failed to render prefill configmap: %w", err)
		}
		manifests["fio-prefill-configmap"] = prefillConfigMap
	}

	// Generate PVCs if storage class is defined
	if m.config.WorkloadArgs.StorageClass != "" {
		for i := 1; i <= m.config.WorkloadArgs.Servers; i++ {
			pvc, err := m.templateEngine.RenderPVC(m.config, i)
			if err != nil {
				return nil, fmt.Errorf("failed to render PVC %d: %w", i, err)
			}
			manifests[fmt.Sprintf("pvc-%d", i)] = pvc
		}
	}

	// Generate server manifests
	for i := 1; i <= m.config.WorkloadArgs.Servers; i++ {
		var server string
		var err error

		if m.config.WorkloadArgs.Kind == "vm" {
			server, err = m.templateEngine.RenderServerVM(m.config, i)
		} else {
			server, err = m.templateEngine.RenderServer(m.config, i)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to render server %d: %w", i, err)
		}
		manifests[fmt.Sprintf("server-%d", i)] = server
	}

	return manifests, nil
}

// RunBenchmark executes the complete FIO distributed benchmark
func (m *Manager) RunBenchmark(ctx context.Context) error {
	log.Println("Starting FIO distributed benchmark execution...")

	// Phase 1: Deploy infrastructure
	if err := m.deployInfrastructure(ctx); err != nil {
		return fmt.Errorf("failed to deploy infrastructure: %w", err)
	}

	// Phase 2: Wait for servers to be ready
	if err := m.waitForServers(ctx); err != nil {
		return fmt.Errorf("failed to wait for servers: %w", err)
	}

	// Phase 3: Create hosts configmap
	if err := m.createHostsConfigMap(ctx); err != nil {
		return fmt.Errorf("failed to create hosts configmap: %w", err)
	}

	// Phase 4: Run prefill if enabled
	if m.config.WorkloadArgs.Prefill {
		if err := m.runPrefill(ctx); err != nil {
			return fmt.Errorf("failed to run prefill: %w", err)
		}
	}

	// Phase 5: Run benchmark
	if err := m.runBenchmarkClient(ctx); err != nil {
		return fmt.Errorf("failed to run benchmark client: %w", err)
	}

	// Phase 6: Wait for completion
	if err := m.waitForCompletion(ctx); err != nil {
		return fmt.Errorf("failed to wait for completion: %w", err)
	}

	m.state = StateCompleted
	log.Println("Benchmark completed successfully!")

	return nil
}

// deployInfrastructure deploys the initial infrastructure
func (m *Manager) deployInfrastructure(ctx context.Context) error {
	log.Println("Deploying infrastructure...")

	// Deploy configmaps
	configMap, err := m.templateEngine.RenderConfigMap(m.config)
	if err != nil {
		return fmt.Errorf("failed to render configmap: %w", err)
	}

	if err := m.k8sClient.ApplyManifest(ctx, configMap, m.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply configmap: %w", err)
	}

	// Deploy prefill configmap if needed
	if m.config.WorkloadArgs.Prefill {
		prefillConfigMap, err := m.templateEngine.RenderPrefillConfigMap(m.config)
		if err != nil {
			return fmt.Errorf("failed to render prefill configmap: %w", err)
		}

		if err := m.k8sClient.ApplyManifest(ctx, prefillConfigMap, m.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply prefill configmap: %w", err)
		}
	}

	// Deploy PVCs if storage class is defined
	if m.config.WorkloadArgs.StorageClass != "" {
		for i := 1; i <= m.config.WorkloadArgs.Servers; i++ {
			pvc, err := m.templateEngine.RenderPVC(m.config, i)
			if err != nil {
				return fmt.Errorf("failed to render PVC %d: %w", i, err)
			}

			if err := m.k8sClient.ApplyManifest(ctx, pvc, m.config.Namespace); err != nil {
				return fmt.Errorf("failed to apply PVC %d: %w", i, err)
			}
		}
	}

	// Deploy servers
	for i := 1; i <= m.config.WorkloadArgs.Servers; i++ {
		var server string
		var err error

		if m.config.WorkloadArgs.Kind == "vm" {
			server, err = m.templateEngine.RenderServerVM(m.config, i)
		} else {
			server, err = m.templateEngine.RenderServer(m.config, i)
		}

		if err != nil {
			return fmt.Errorf("failed to render server %d: %w", i, err)
		}

		if err := m.k8sClient.ApplyManifest(ctx, server, m.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply server %d: %w", i, err)
		}
	}

	m.state = StateStartingServers
	return nil
}

// waitForServers waits for all FIO servers to be ready
func (m *Manager) waitForServers(ctx context.Context) error {
	log.Printf("Waiting for %d FIO servers to be ready...", m.config.WorkloadArgs.Servers)

	labelSelector := fmt.Sprintf("app=fio-benchmark-%s", m.config.GetTruncatedUUID())
	timeout := time.Duration(m.config.WorkloadArgs.JobTimeout) * time.Second

	if err := m.k8sClient.WaitForPodsReady(ctx, m.config.Namespace, labelSelector, m.config.WorkloadArgs.Servers, timeout); err != nil {
		return fmt.Errorf("failed to wait for servers to be ready: %w", err)
	}

	// Get pod IPs and node names
	podDetails, err := m.k8sClient.GetPodIPs(ctx, m.config.Namespace, labelSelector)
	if err != nil {
		return fmt.Errorf("failed to get pod IPs: %w", err)
	}

	if len(podDetails) != m.config.WorkloadArgs.Servers {
		return fmt.Errorf("expected %d servers, got %d", m.config.WorkloadArgs.Servers, len(podDetails))
	}

	m.podDetails = podDetails
	m.state = StateStartingClient

	log.Printf("All %d servers are ready", len(podDetails))
	return nil
}

// createHostsConfigMap creates the hosts configmap for FIO clients
func (m *Manager) createHostsConfigMap(ctx context.Context) error {
	log.Println("Creating hosts configmap...")

	var hosts []string
	for ip := range m.podDetails {
		hosts = append(hosts, ip)
	}

	hostsConfigMap, err := m.templateEngine.RenderHostsConfigMap(m.config, hosts)
	if err != nil {
		return fmt.Errorf("failed to render hosts configmap: %w", err)
	}

	if err := m.k8sClient.ApplyManifest(ctx, hostsConfigMap, m.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply hosts configmap: %w", err)
	}

	// Wait for FIO server port if VMs
	if m.config.WorkloadArgs.Kind == "vm" {
		log.Println("Waiting for FIO server port 8765 to be ready on VMs...")
		// Note: In a real implementation, you would add port checking logic here
		time.Sleep(30 * time.Second) // Simple wait for demo purposes
	}

	return nil
}

// runPrefill runs the prefill job if enabled
func (m *Manager) runPrefill(ctx context.Context) error {
	log.Println("Running prefill job...")

	m.state = StatePrefilling

	prefillClient, err := m.templateEngine.RenderPrefillClient(m.config)
	if err != nil {
		return fmt.Errorf("failed to render prefill client: %w", err)
	}

	if err := m.k8sClient.ApplyManifest(ctx, prefillClient, m.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply prefill client: %w", err)
	}

	// Wait for prefill job to complete
	jobName := fmt.Sprintf("fio-prefill-%s", m.config.GetTruncatedUUID())
	timeout := time.Duration(m.config.WorkloadArgs.JobTimeout) * time.Second

	if err := m.k8sClient.WaitForJobCompletion(ctx, jobName, m.config.Namespace, timeout); err != nil {
		return fmt.Errorf("prefill job failed: %w", err)
	}

	// Sleep after prefill if configured
	if m.config.WorkloadArgs.PostPrefillSleep > 0 {
		log.Printf("Sleeping for %d seconds after prefill...", m.config.WorkloadArgs.PostPrefillSleep)
		time.Sleep(time.Duration(m.config.WorkloadArgs.PostPrefillSleep) * time.Second)
	}

	m.state = StateStartBenchmark
	log.Println("Prefill completed successfully")

	return nil
}

// runBenchmarkClient runs the main benchmark client
func (m *Manager) runBenchmarkClient(ctx context.Context) error {
	log.Println("Starting benchmark client...")

	if !m.config.WorkloadArgs.Prefill {
		m.state = StateStartBenchmark
	}

	client, err := m.templateEngine.RenderClient(m.config, m.podDetails)
	if err != nil {
		return fmt.Errorf("failed to render client: %w", err)
	}

	if err := m.k8sClient.ApplyManifest(ctx, client, m.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply client: %w", err)
	}

	m.state = StateRunning
	log.Println("Benchmark client started")

	return nil
}

// waitForCompletion waits for the benchmark to complete
func (m *Manager) waitForCompletion(ctx context.Context) error {
	log.Println("Waiting for benchmark to complete...")

	jobName := fmt.Sprintf("fio-client-%s", m.config.GetTruncatedUUID())
	timeout := time.Duration(m.config.WorkloadArgs.JobTimeout) * time.Second

	if err := m.k8sClient.WaitForJobCompletion(ctx, jobName, m.config.Namespace, timeout); err != nil {
		return fmt.Errorf("benchmark job failed: %w", err)
	}

	log.Println("Benchmark completed successfully!")
	return nil
}

// Cleanup removes all resources created by the benchmark
func (m *Manager) Cleanup(ctx context.Context) error {
	log.Println("Cleaning up benchmark resources...")

	labelSelector := fmt.Sprintf("benchmark-uuid=%s", m.config.UUID)

	if err := m.k8sClient.CleanupResources(ctx, m.config.Namespace, labelSelector); err != nil {
		return fmt.Errorf("failed to cleanup resources: %w", err)
	}

	// Also cleanup by truncated UUID selector
	labelSelector = fmt.Sprintf("app=fio-benchmark-%s", m.config.GetTruncatedUUID())
	if err := m.k8sClient.CleanupResources(ctx, m.config.Namespace, labelSelector); err != nil {
		log.Printf("Warning: failed to cleanup resources with label %s: %v", labelSelector, err)
	}

	log.Println("Cleanup completed")
	return nil
}

// GetState returns the current benchmark state
func (m *Manager) GetState() State {
	return m.state
}
