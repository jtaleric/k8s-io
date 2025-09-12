package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
	"github.com/jtaleric/k8s-io/pkg/workloads"
)

func main() {
	var (
		configFile = flag.String("config", "config.yaml", "Path to configuration file")
		cleanup    = flag.Bool("cleanup", false, "Cleanup resources and exit")
		dryRun     = flag.Bool("dry-run", false, "Generate manifests without applying them")
	)
	flag.Parse()

	log.Println("Starting K8s-IO benchmark tool...")

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration for workload: %s", cfg.Workload.Name)

	// Create Kubernetes client
	k8sClient, err := kubernetes.NewClient()
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	// Create workload factory and workload
	factory := workloads.NewFactory(k8sClient, cfg)
	workload, err := factory.CreateWorkload()
	if err != nil {
		log.Fatalf("Failed to create workload: %v", err)
	}

	log.Printf("Created %s workload", workload.GetName())

	// Handle cleanup
	if *cleanup {
		log.Println("Cleaning up resources...")
		ctx := context.Background()
		if err := workload.Cleanup(ctx); err != nil {
			log.Fatalf("Cleanup failed: %v", err)
		}
		log.Println("Cleanup completed successfully!")
		return
	}

	// Handle dry-run
	if *dryRun {
		log.Println("Generating manifests (dry-run mode)...")
		manifests, err := workload.GenerateManifests()
		if err != nil {
			log.Fatalf("Failed to generate manifests: %v", err)
		}

		fmt.Println("\n=== Generated Manifests ===")
		for name, manifest := range manifests {
			fmt.Printf("\n--- %s ---\n", name)
			fmt.Println(manifest)
		}
		return
	}

	// Ensure namespace exists (only for actual benchmark runs)
	ctx := context.Background()
	exists, err := k8sClient.NamespaceExists(ctx, cfg.Namespace)
	if err != nil {
		log.Fatalf("Failed to check if namespace exists: %v", err)
	}

	if !exists {
		log.Printf("Creating namespace: %s", cfg.Namespace)
		if err := k8sClient.CreateNamespace(ctx, cfg.Namespace); err != nil {
			log.Fatalf("Failed to create namespace: %v", err)
		}
	}

	// Run the benchmark
	log.Printf("Starting %s benchmark...", workload.GetName())
	if err := workload.RunBenchmark(ctx); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	log.Println("Benchmark completed successfully!")
}
