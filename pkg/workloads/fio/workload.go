package fio

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
)

// Workload implements the FIO distributed benchmark workload
type Workload struct {
	k8sClient      *kubernetes.Client
	templateEngine *TemplateEngine
	config         *config.Config
	fioConfig      *FIOConfig
	podDetails     map[string]string
}

// NewWorkload creates a new FIO workload
func NewWorkload(k8sClient *kubernetes.Client, cfg *config.Config, fioConfig *FIOConfig) (*Workload, error) {
	templateEngine := NewTemplateEngine("pkg/workloads/fio/templates")

	return &Workload{
		k8sClient:      k8sClient,
		templateEngine: templateEngine,
		config:         cfg,
		fioConfig:      fioConfig,
		podDetails:     make(map[string]string),
	}, nil
}

// GetName returns the workload name
func (w *Workload) GetName() string {
	return "fio"
}

// Validate validates the workload configuration
func (w *Workload) Validate() error {
	return w.fioConfig.Validate()
}

// GenerateManifests generates all Kubernetes manifests
func (w *Workload) GenerateManifests() (map[string]string, error) {
	manifests := make(map[string]string)

	// Generate FIO test configmap
	configMap, err := w.templateEngine.RenderFIOConfigMap(w.config, w.fioConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to render configmap: %w", err)
	}
	manifests["fio-configmap"] = configMap

	// Generate prefill configmap if prefill is enabled
	if w.fioConfig.Prefill {
		prefillConfigMap, err := w.templateEngine.RenderFIOPrefillConfigMap(w.config, w.fioConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to render prefill configmap: %w", err)
		}
		manifests["fio-prefill-configmap"] = prefillConfigMap
	}

	// Generate PVCs if storage class is defined
	if w.fioConfig.StorageClass != "" {
		for i := 1; i <= w.fioConfig.Servers; i++ {
			pvc, err := w.templateEngine.RenderFIOPVC(w.config, w.fioConfig, i)
			if err != nil {
				return nil, fmt.Errorf("failed to render PVC %d: %w", i, err)
			}
			manifests[fmt.Sprintf("pvc-%d", i)] = pvc
		}
	}

	// Generate server manifests
	for i := 1; i <= w.fioConfig.Servers; i++ {
		var server string
		var err error

		if w.fioConfig.Kind == "vm" {
			server, err = w.templateEngine.RenderFIOServerVM(w.config, w.fioConfig, i)
		} else {
			server, err = w.templateEngine.RenderFIOServer(w.config, w.fioConfig, i)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to render server %d: %w", i, err)
		}
		manifests[fmt.Sprintf("server-%d", i)] = server
	}

	// Generate client job (for dry-run purposes, use mock pod details)
	mockPodDetails := make(map[string]string)
	for i := 1; i <= w.fioConfig.Servers; i++ {
		mockPodDetails[fmt.Sprintf("10.0.0.%d", i)] = fmt.Sprintf("worker-%d", i)
	}

	client, err := w.templateEngine.RenderFIOClientWithPrometheus(w.config, w.fioConfig, mockPodDetails, w.k8sClient)
	if err != nil {
		return nil, fmt.Errorf("failed to render client: %w", err)
	}
	manifests["fio-client"] = client

	// Generate prefill client if prefill is enabled
	if w.fioConfig.Prefill {
		prefillClient, err := w.templateEngine.RenderFIOPrefillClient(w.config, w.fioConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to render prefill client: %w", err)
		}
		manifests["fio-prefill-client"] = prefillClient
	}

	return manifests, nil
}

// RunBenchmark executes the complete FIO distributed benchmark
func (w *Workload) RunBenchmark(ctx context.Context) error {
	log.Println("Starting FIO distributed benchmark execution...")

	// Phase 1: Deploy infrastructure
	if err := w.deployInfrastructure(ctx); err != nil {
		return fmt.Errorf("failed to deploy infrastructure: %w", err)
	}

	// Phase 2: Wait for servers to be ready
	if err := w.waitForServers(ctx); err != nil {
		return fmt.Errorf("failed to wait for servers: %w", err)
	}

	// Phase 3: Create hosts configmap
	if err := w.createHostsConfigMap(ctx); err != nil {
		return fmt.Errorf("failed to create hosts configmap: %w", err)
	}

	// Phase 4: Run prefill if enabled
	if w.fioConfig.Prefill {
		if err := w.runPrefill(ctx); err != nil {
			return fmt.Errorf("failed to run prefill: %w", err)
		}
	}

	// Phase 5: Run benchmark
	if err := w.runBenchmarkClient(ctx); err != nil {
		return fmt.Errorf("failed to run benchmark client: %w", err)
	}

	// Phase 6: Wait for completion
	if err := w.waitForCompletion(ctx); err != nil {
		return fmt.Errorf("failed to wait for completion: %w", err)
	}

	log.Println("Benchmark completed successfully!")

	return nil
}

// deployInfrastructure deploys the initial infrastructure
func (w *Workload) deployInfrastructure(ctx context.Context) error {
	log.Println("Deploying infrastructure...")

	// Deploy configmaps
	configMap, err := w.templateEngine.RenderFIOConfigMap(w.config, w.fioConfig)
	if err != nil {
		return fmt.Errorf("failed to render configmap: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, configMap, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply configmap: %w", err)
	}

	// Deploy prefill configmap if needed
	if w.fioConfig.Prefill {
		prefillConfigMap, err := w.templateEngine.RenderFIOPrefillConfigMap(w.config, w.fioConfig)
		if err != nil {
			return fmt.Errorf("failed to render prefill configmap: %w", err)
		}

		if err := w.k8sClient.ApplyManifest(ctx, prefillConfigMap, w.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply prefill configmap: %w", err)
		}
	}

	// Deploy PVCs if storage class is defined
	if w.fioConfig.StorageClass != "" {
		for i := 1; i <= w.fioConfig.Servers; i++ {
			pvc, err := w.templateEngine.RenderFIOPVC(w.config, w.fioConfig, i)
			if err != nil {
				return fmt.Errorf("failed to render PVC %d: %w", i, err)
			}

			if err := w.k8sClient.ApplyManifest(ctx, pvc, w.config.Namespace); err != nil {
				return fmt.Errorf("failed to apply PVC %d: %w", i, err)
			}
		}
	}

	// Deploy servers
	for i := 1; i <= w.fioConfig.Servers; i++ {
		var server string
		var err error

		if w.fioConfig.Kind == "vm" {
			server, err = w.templateEngine.RenderFIOServerVM(w.config, w.fioConfig, i)
		} else {
			server, err = w.templateEngine.RenderFIOServer(w.config, w.fioConfig, i)
		}

		if err != nil {
			return fmt.Errorf("failed to render server %d: %w", i, err)
		}

		if err := w.k8sClient.ApplyManifest(ctx, server, w.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply server %d: %w", i, err)
		}
	}

	return nil
}

// waitForServers waits for all FIO servers to be ready
func (w *Workload) waitForServers(ctx context.Context) error {
	log.Printf("Waiting for %d FIO servers to be ready...", w.fioConfig.Servers)

	labelSelector := fmt.Sprintf("app=fio-benchmark-%s", w.config.GetTruncatedUUID())
	timeout := time.Duration(w.fioConfig.JobTimeout) * time.Second

	if err := w.k8sClient.WaitForPodsReady(ctx, w.config.Namespace, labelSelector, w.fioConfig.Servers, timeout); err != nil {
		return fmt.Errorf("failed to wait for servers to be ready: %w", err)
	}

	// Get pod IPs and node names
	podDetails, err := w.k8sClient.GetPodIPs(ctx, w.config.Namespace, labelSelector)
	if err != nil {
		return fmt.Errorf("failed to get pod IPs: %w", err)
	}

	if len(podDetails) != w.fioConfig.Servers {
		return fmt.Errorf("expected %d servers, got %d", w.fioConfig.Servers, len(podDetails))
	}

	w.podDetails = podDetails

	log.Printf("All %d servers are ready", len(podDetails))
	return nil
}

// createHostsConfigMap creates the hosts configmap for FIO clients
func (w *Workload) createHostsConfigMap(ctx context.Context) error {
	log.Println("Creating hosts configmap...")

	var hosts []string
	for ip := range w.podDetails {
		hosts = append(hosts, ip)
	}

	hostsConfigMap, err := w.templateEngine.RenderHostsConfigMap(w.config, hosts)
	if err != nil {
		return fmt.Errorf("failed to render hosts configmap: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, hostsConfigMap, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply hosts configmap: %w", err)
	}

	// Wait for FIO server port if VMs
	if w.fioConfig.Kind == "vm" {
		log.Println("Waiting for FIO server port 8765 to be ready on VMs...")
		time.Sleep(30 * time.Second)
	}

	return nil
}

// runPrefill runs the prefill job if enabled
func (w *Workload) runPrefill(ctx context.Context) error {
	log.Println("Running prefill job...")

	prefillClient, err := w.templateEngine.RenderFIOPrefillClient(w.config, w.fioConfig)
	if err != nil {
		return fmt.Errorf("failed to render prefill client: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, prefillClient, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply prefill client: %w", err)
	}

	// Wait for prefill job to complete
	jobName := fmt.Sprintf("fio-prefill-%s", w.config.GetTruncatedUUID())
	timeout := time.Duration(w.fioConfig.JobTimeout) * time.Second

	if err := w.k8sClient.WaitForJobCompletion(ctx, jobName, w.config.Namespace, timeout); err != nil {
		return fmt.Errorf("prefill job failed: %w", err)
	}

	// Sleep after prefill if configured
	if w.fioConfig.PostPrefillSleep > 0 {
		log.Printf("Sleeping for %d seconds after prefill...", w.fioConfig.PostPrefillSleep)
		time.Sleep(time.Duration(w.fioConfig.PostPrefillSleep) * time.Second)
	}

	log.Println("Prefill completed successfully")

	return nil
}

// runBenchmarkClient runs the main benchmark client
func (w *Workload) runBenchmarkClient(ctx context.Context) error {
	log.Println("Starting benchmark client...")

	client, err := w.templateEngine.RenderFIOClientWithPrometheus(w.config, w.fioConfig, w.podDetails, w.k8sClient)
	if err != nil {
		return fmt.Errorf("failed to render client: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, client, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply client: %w", err)
	}

	log.Println("Benchmark client started")

	return nil
}

// waitForCompletion waits for the benchmark to complete
func (w *Workload) waitForCompletion(ctx context.Context) error {
	log.Println("Waiting for benchmark to complete...")

	jobName := fmt.Sprintf("fio-client-%s", w.config.GetTruncatedUUID())
	timeout := time.Duration(w.fioConfig.JobTimeout) * time.Second

	if err := w.k8sClient.WaitForJobCompletion(ctx, jobName, w.config.Namespace, timeout); err != nil {
		return fmt.Errorf("benchmark job failed: %w", err)
	}

	log.Println("Benchmark completed successfully!")
	return nil
}

// Cleanup removes all resources created by the benchmark
func (w *Workload) Cleanup(ctx context.Context) error {
	log.Println("Cleaning up benchmark resources...")

	labelSelector := fmt.Sprintf("benchmark-uuid=%s", w.config.UUID)

	if err := w.k8sClient.CleanupResources(ctx, w.config.Namespace, labelSelector); err != nil {
		return fmt.Errorf("failed to cleanup resources: %w", err)
	}

	// Also cleanup by truncated UUID selector
	labelSelector = fmt.Sprintf("app=fio-benchmark-%s", w.config.GetTruncatedUUID())
	if err := w.k8sClient.CleanupResources(ctx, w.config.Namespace, labelSelector); err != nil {
		log.Printf("Warning: failed to cleanup resources with label %s: %v", labelSelector, err)
	}

	log.Println("Cleanup completed")
	return nil
}
