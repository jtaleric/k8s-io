package hammerdb

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
)

// Workload implements the HammerDB benchmark workload
type Workload struct {
	k8sClient      *kubernetes.Client
	templateEngine *TemplateEngine
	config         *config.Config
	hammerdbConfig *HammerDBConfig
}

// NewWorkload creates a new HammerDB workload
func NewWorkload(k8sClient *kubernetes.Client, cfg *config.Config, hammerdbConfig *HammerDBConfig) (*Workload, error) {
	templateEngine := NewTemplateEngine("pkg/workloads/hammerdb/templates")

	return &Workload{
		k8sClient:      k8sClient,
		templateEngine: templateEngine,
		config:         cfg,
		hammerdbConfig: hammerdbConfig,
	}, nil
}

// GetName returns the workload name
func (w *Workload) GetName() string {
	return "hammerdb"
}

// Validate validates the workload configuration
func (w *Workload) Validate() error {
	return w.hammerdbConfig.Validate()
}

// GenerateManifests generates all Kubernetes manifests
func (w *Workload) GenerateManifests() (map[string]string, error) {
	manifests := make(map[string]string)

	// Generate PVC if client VM PVC is enabled
	if w.hammerdbConfig.ClientVM.PVC {
		pvc, err := w.templateEngine.RenderHammerDBPVC(w.config, w.hammerdbConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to render PVC: %w", err)
		}
		manifests["hammerdb-pvc"] = pvc
	}

	// Generate database creation script configmap
	createDBScript, err := w.templateEngine.RenderHammerDBCreateScript(w.config, w.hammerdbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to render create DB script: %w", err)
	}
	manifests["hammerdb-createdb-script"] = createDBScript

	// Generate workload script configmap
	workloadScript, err := w.templateEngine.RenderHammerDBWorkloadScript(w.config, w.hammerdbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to render workload script: %w", err)
	}
	manifests["hammerdb-workload-script"] = workloadScript

	// Generate VM workload script if needed
	if w.hammerdbConfig.Kind == "vm" {
		vmScript, err := w.templateEngine.RenderHammerDBVMWorkloadScript(w.config, w.hammerdbConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to render VM workload script: %w", err)
		}
		manifests["hammerdb-vm-workload-script"] = vmScript
	}

	return manifests, nil
}

// RunBenchmark executes the complete HammerDB benchmark
func (w *Workload) RunBenchmark(ctx context.Context) error {
	log.Println("Starting HammerDB benchmark execution...")

	// Phase 1: Deploy infrastructure
	if err := w.deployInfrastructure(ctx); err != nil {
		return fmt.Errorf("failed to deploy infrastructure: %w", err)
	}

	// Phase 2: Run database initialization if enabled
	if w.hammerdbConfig.DBInit {
		if err := w.runDBInitialization(ctx); err != nil {
			return fmt.Errorf("failed to initialize database: %w", err)
		}
	}

	// Phase 3: Run benchmark if enabled
	if w.hammerdbConfig.DBBenchmark {
		if err := w.runBenchmark(ctx); err != nil {
			return fmt.Errorf("failed to run benchmark: %w", err)
		}
	}

	// Phase 4: Wait for completion
	if w.hammerdbConfig.DBBenchmark {
		if err := w.waitForCompletion(ctx); err != nil {
			return fmt.Errorf("failed to wait for completion: %w", err)
		}
	}

	log.Println("HammerDB benchmark completed successfully!")

	return nil
}

// deployInfrastructure deploys the initial infrastructure
func (w *Workload) deployInfrastructure(ctx context.Context) error {
	log.Println("Deploying HammerDB infrastructure...")

	// Deploy PVC if needed
	if w.hammerdbConfig.ClientVM.PVC {
		pvc, err := w.templateEngine.RenderHammerDBPVC(w.config, w.hammerdbConfig)
		if err != nil {
			return fmt.Errorf("failed to render PVC: %w", err)
		}

		if err := w.k8sClient.ApplyManifest(ctx, pvc, w.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply PVC: %w", err)
		}
	}

	// Deploy create DB script configmap
	createDBScript, err := w.templateEngine.RenderHammerDBCreateScript(w.config, w.hammerdbConfig)
	if err != nil {
		return fmt.Errorf("failed to render create DB script: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, createDBScript, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply create DB script: %w", err)
	}

	// Deploy workload script configmap
	workloadScript, err := w.templateEngine.RenderHammerDBWorkloadScript(w.config, w.hammerdbConfig)
	if err != nil {
		return fmt.Errorf("failed to render workload script: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, workloadScript, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply workload script: %w", err)
	}

	// Deploy VM workload script if needed
	if w.hammerdbConfig.Kind == "vm" {
		vmScript, err := w.templateEngine.RenderHammerDBVMWorkloadScript(w.config, w.hammerdbConfig)
		if err != nil {
			return fmt.Errorf("failed to render VM workload script: %w", err)
		}

		if err := w.k8sClient.ApplyManifest(ctx, vmScript, w.config.Namespace); err != nil {
			return fmt.Errorf("failed to apply VM workload script: %w", err)
		}
	}

	return nil
}

// runDBInitialization runs the database initialization job
func (w *Workload) runDBInitialization(ctx context.Context) error {
	log.Println("Running database initialization...")

	var dbCreationJob string
	var err error

	if w.hammerdbConfig.Kind == "vm" {
		// For VM, use the VM-specific template
		switch w.hammerdbConfig.DBType {
		case "mssql":
			dbCreationJob, err = w.templateEngine.RenderHammerDBCreateJobVM(w.config, w.hammerdbConfig, "mssql")
		case "mariadb":
			dbCreationJob, err = w.templateEngine.RenderHammerDBCreateJobVM(w.config, w.hammerdbConfig, "mariadb")
		case "pg":
			dbCreationJob, err = w.templateEngine.RenderHammerDBCreateJobVM(w.config, w.hammerdbConfig, "postgres")
		}
	} else {
		// For pod, use the generic template
		dbCreationJob, err = w.templateEngine.RenderHammerDBCreateJob(w.config, w.hammerdbConfig)
	}

	if err != nil {
		return fmt.Errorf("failed to render DB creation job: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, dbCreationJob, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply DB creation job: %w", err)
	}

	// Wait for DB creation to complete
	jobName := fmt.Sprintf("hammerdb-creator-%s", w.config.GetTruncatedUUID())
	timeout := time.Duration(w.hammerdbConfig.JobTimeout) * time.Second

	if err := w.k8sClient.WaitForJobCompletion(ctx, jobName, w.config.Namespace, timeout); err != nil {
		return fmt.Errorf("DB creation job failed: %w", err)
	}

	log.Println("Database initialization completed successfully")
	return nil
}

// runBenchmark runs the main benchmark workload
func (w *Workload) runBenchmark(ctx context.Context) error {
	log.Println("Starting HammerDB benchmark workload...")

	var workloadJob string
	var err error

	// Generate the appropriate workload job based on database type
	switch w.hammerdbConfig.DBType {
	case "mssql":
		workloadJob, err = w.templateEngine.RenderHammerDBWorkloadJob(w.config, w.hammerdbConfig, "mssql")
	case "mariadb":
		workloadJob, err = w.templateEngine.RenderHammerDBWorkloadJob(w.config, w.hammerdbConfig, "mariadb")
	case "pg":
		workloadJob, err = w.templateEngine.RenderHammerDBWorkloadJob(w.config, w.hammerdbConfig, "postgres")
	}

	if err != nil {
		return fmt.Errorf("failed to render workload job: %w", err)
	}

	if err := w.k8sClient.ApplyManifest(ctx, workloadJob, w.config.Namespace); err != nil {
		return fmt.Errorf("failed to apply workload job: %w", err)
	}

	log.Println("HammerDB benchmark workload started")

	return nil
}

// waitForCompletion waits for the benchmark to complete
func (w *Workload) waitForCompletion(ctx context.Context) error {
	log.Println("Waiting for HammerDB benchmark to complete...")

	// The job name depends on the database type
	var jobName string
	switch w.hammerdbConfig.DBType {
	case "mssql":
		jobName = fmt.Sprintf("hammerdb-mssql-workload-%s", w.config.GetTruncatedUUID())
	case "mariadb":
		jobName = fmt.Sprintf("hammerdb-mariadb-workload-%s", w.config.GetTruncatedUUID())
	case "pg":
		jobName = fmt.Sprintf("hammerdb-postgres-workload-%s", w.config.GetTruncatedUUID())
	}

	timeout := time.Duration(w.hammerdbConfig.JobTimeout) * time.Second

	if err := w.k8sClient.WaitForJobCompletion(ctx, jobName, w.config.Namespace, timeout); err != nil {
		return fmt.Errorf("benchmark job failed: %w", err)
	}

	log.Println("HammerDB benchmark completed successfully!")
	return nil
}

// Cleanup removes all resources created by the benchmark
func (w *Workload) Cleanup(ctx context.Context) error {
	log.Println("Cleaning up HammerDB benchmark resources...")

	labelSelector := fmt.Sprintf("benchmark-uuid=%s", w.config.UUID)

	if err := w.k8sClient.CleanupResources(ctx, w.config.Namespace, labelSelector); err != nil {
		return fmt.Errorf("failed to cleanup resources: %w", err)
	}

	log.Println("Cleanup completed")
	return nil
}
