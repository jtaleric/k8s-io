package kubernetes

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/yaml"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// PrometheusInfo holds discovered Prometheus configuration
type PrometheusInfo struct {
	URL   string
	Token string
	Found bool
}

// Client wraps Kubernetes client functionality
type Client struct {
	clientset     kubernetes.Interface
	dynamicClient dynamic.Interface
	config        *rest.Config
}

// NewClient creates a new Kubernetes client
func NewClient() (*Client, error) {
	config, err := getKubeConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes clientset: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &Client{
		clientset:     clientset,
		dynamicClient: dynamicClient,
		config:        config,
	}, nil
}

// getKubeConfig gets the Kubernetes configuration
func getKubeConfig() (*rest.Config, error) {
	// Try in-cluster config first
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}

	// Fall back to kubeconfig file
	var kubeconfig string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	// Override with KUBECONFIG env var if set
	if kubeconfigEnv := os.Getenv("KUBECONFIG"); kubeconfigEnv != "" {
		kubeconfig = kubeconfigEnv
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	return config, nil
}

// ApplyManifest applies a YAML manifest to the cluster
func (c *Client) ApplyManifest(ctx context.Context, manifestYAML string, namespace string) error {
	// Parse the YAML into an unstructured object
	obj := &unstructured.Unstructured{}
	dec := yaml.NewDecodingSerializer(unstructured.UnstructuredJSONScheme)

	_, gvk, err := dec.Decode([]byte(manifestYAML), nil, obj)
	if err != nil {
		return fmt.Errorf("failed to decode manifest: %w", err)
	}

	// Set namespace if not specified in manifest
	if obj.GetNamespace() == "" && namespace != "" {
		obj.SetNamespace(namespace)
	}

	// Get the appropriate resource interface
	gvr := schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: pluralizeResource(gvk.Kind),
	}

	var resourceClient dynamic.ResourceInterface
	if obj.GetNamespace() != "" {
		resourceClient = c.dynamicClient.Resource(gvr).Namespace(obj.GetNamespace())
	} else {
		resourceClient = c.dynamicClient.Resource(gvr)
	}

	// Try to get the existing resource
	existing, err := resourceClient.Get(ctx, obj.GetName(), metav1.GetOptions{})
	if err != nil {
		// Resource doesn't exist, create it
		_, err = resourceClient.Create(ctx, obj, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create resource %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	} else {
		// Resource exists, update it
		obj.SetResourceVersion(existing.GetResourceVersion())
		_, err = resourceClient.Update(ctx, obj, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to update resource %s/%s: %w", obj.GetKind(), obj.GetName(), err)
		}
	}

	return nil
}

// DeleteResource deletes a resource by name, kind, and namespace
func (c *Client) DeleteResource(ctx context.Context, kind, name, namespace string) error {
	gvk := getGVKForKind(kind)
	gvr := schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: pluralizeResource(kind),
	}

	var resourceClient dynamic.ResourceInterface
	if namespace != "" {
		resourceClient = c.dynamicClient.Resource(gvr).Namespace(namespace)
	} else {
		resourceClient = c.dynamicClient.Resource(gvr)
	}

	err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete %s/%s: %w", kind, name, err)
	}

	return nil
}

// NamespaceExists checks if a namespace exists
func (c *Client) NamespaceExists(ctx context.Context, namespace string) (bool, error) {
	_, err := c.clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}
	return true, nil
}

// CreateNamespace creates a namespace
func (c *Client) CreateNamespace(ctx context.Context, namespace string) error {
	_, err := c.clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create namespace %s: %w", namespace, err)
	}

	return nil
}

// ListPods lists pods with the given label selector
func (c *Client) ListPods(ctx context.Context, namespace string, labelSelector string) (*corev1.PodList, error) {
	return c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
}

// GetJob gets a job by name and namespace
func (c *Client) GetJob(ctx context.Context, name, namespace string) (*batchv1.Job, error) {
	return c.clientset.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
}

// WaitForPodsReady waits for pods to be ready
func (c *Client) WaitForPodsReady(ctx context.Context, namespace string, labelSelector string, expectedCount int, timeout time.Duration) error {
	return wait.PollImmediate(5*time.Second, timeout, func() (bool, error) {
		pods, err := c.ListPods(ctx, namespace, labelSelector)
		if err != nil {
			return false, err
		}

		readyCount := 0
		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning {
				readyCount++
			}
		}

		return readyCount >= expectedCount, nil
	})
}

// WaitForJobCompletion waits for a job to complete
func (c *Client) WaitForJobCompletion(ctx context.Context, name, namespace string, timeout time.Duration) error {
	return wait.PollImmediate(10*time.Second, timeout, func() (bool, error) {
		job, err := c.GetJob(ctx, name, namespace)
		if err != nil {
			return false, err
		}

		// Check if job completed successfully
		if job.Status.Succeeded > 0 {
			return true, nil
		}

		// Check if job failed
		if job.Status.Failed > 0 {
			return false, fmt.Errorf("job %s failed", name)
		}

		return false, nil
	})
}

// CleanupResources deletes resources with the given label selector
func (c *Client) CleanupResources(ctx context.Context, namespace string, labelSelector string) error {
	// Delete pods
	err := c.clientset.CoreV1().Pods(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to delete pods: %w", err)
	}

	// Delete jobs
	err = c.clientset.BatchV1().Jobs(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to delete jobs: %w", err)
	}

	// Delete configmaps
	err = c.clientset.CoreV1().ConfigMaps(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to delete configmaps: %w", err)
	}

	// Delete PVCs
	err = c.clientset.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to delete PVCs: %w", err)
	}

	return nil
}

// GetPodIPs gets the IP addresses of pods matching the label selector
func (c *Client) GetPodIPs(ctx context.Context, namespace string, labelSelector string) (map[string]string, error) {
	pods, err := c.ListPods(ctx, namespace, labelSelector)
	if err != nil {
		return nil, err
	}

	podDetails := make(map[string]string)
	for _, pod := range pods.Items {
		if pod.Status.PodIP != "" && pod.Spec.NodeName != "" {
			podDetails[pod.Status.PodIP] = pod.Spec.NodeName
		}
	}

	return podDetails, nil
}

// pluralizeResource converts a resource kind to its plural form
func pluralizeResource(kind string) string {
	switch kind {
	case "Pod":
		return "pods"
	case "Job":
		return "jobs"
	case "ConfigMap":
		return "configmaps"
	case "PersistentVolumeClaim":
		return "persistentvolumeclaims"
	case "VirtualMachineInstance":
		return "virtualmachineinstances"
	default:
		// Simple pluralization - add 's'
		return strings.ToLower(kind) + "s"
	}
}

// getGVKForKind returns the GroupVersionKind for a given kind
func getGVKForKind(kind string) schema.GroupVersionKind {
	switch kind {
	case "Pod":
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}
	case "Job":
		return schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}
	case "ConfigMap":
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	case "PersistentVolumeClaim":
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"}
	case "VirtualMachineInstance":
		return schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1alpha3", Kind: "VirtualMachineInstance"}
	default:
		return schema.GroupVersionKind{Group: "", Version: "v1", Kind: kind}
	}
}

// DiscoverPrometheus attempts to discover Prometheus configuration in the cluster
func (c *Client) DiscoverPrometheus(ctx context.Context) (*PrometheusInfo, error) {
	info := &PrometheusInfo{Found: false}

	// Common Prometheus service names and namespaces to check
	targets := []struct {
		namespace   string
		serviceName string
		port        string
	}{
		{"openshift-monitoring", "prometheus-k8s", "9090"},
		{"monitoring", "prometheus-server", "80"},
		{"prometheus", "prometheus-server", "9090"},
		{"kube-system", "prometheus", "9090"},
		{"default", "prometheus", "9090"},
	}

	for _, target := range targets {
		// Try to get the service
		service, err := c.clientset.CoreV1().Services(target.namespace).Get(ctx, target.serviceName, metav1.GetOptions{})
		if err != nil {
			continue // Service not found, try next target
		}

		// Found a Prometheus service
		info.Found = true
		info.URL = fmt.Sprintf("http://%s.%s.svc.cluster.local:%s", service.Name, service.Namespace, target.port)

		// Try to get service account token for Prometheus access
		token, err := c.getPrometheusToken(ctx, target.namespace)
		if err == nil {
			info.Token = token
		}

		return info, nil
	}

	// If no service found, try to detect Prometheus via routes (OpenShift)
	if routeInfo, err := c.discoverPrometheusRoute(ctx); err == nil && routeInfo.Found {
		return routeInfo, nil
	}

	return info, nil
}

// getPrometheusToken attempts to get a service account token for Prometheus access
func (c *Client) getPrometheusToken(ctx context.Context, namespace string) (string, error) {
	// Try common service account names used by Prometheus
	saNames := []string{"prometheus", "prometheus-server", "default"}

	for _, saName := range saNames {
		sa, err := c.clientset.CoreV1().ServiceAccounts(namespace).Get(ctx, saName, metav1.GetOptions{})
		if err != nil {
			continue
		}

		// Get the first secret (token) associated with this service account
		if len(sa.Secrets) > 0 {
			secretName := sa.Secrets[0].Name
			secret, err := c.clientset.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
			if err != nil {
				continue
			}

			if token, exists := secret.Data["token"]; exists {
				return string(token), nil
			}
		}
	}

	return "", fmt.Errorf("no service account token found for Prometheus")
}

// discoverPrometheusRoute attempts to discover Prometheus via OpenShift routes
func (c *Client) discoverPrometheusRoute(ctx context.Context) (*PrometheusInfo, error) {
	info := &PrometheusInfo{Found: false}

	// Try to get Prometheus route in openshift-monitoring namespace
	routeGVR := schema.GroupVersionResource{
		Group:    "route.openshift.io",
		Version:  "v1",
		Resource: "routes",
	}

	routes, err := c.dynamicClient.Resource(routeGVR).Namespace("openshift-monitoring").List(ctx, metav1.ListOptions{})
	if err != nil {
		return info, err
	}

	for _, route := range routes.Items {
		if strings.Contains(route.GetName(), "prometheus") {
			if spec, found, err := unstructured.NestedMap(route.Object, "spec"); found && err == nil {
				if host, exists := spec["host"].(string); exists {
					info.Found = true
					info.URL = fmt.Sprintf("https://%s", host)

					// For OpenShift, try to get the current user's token
					if token, err := c.getCurrentUserToken(ctx); err == nil {
						info.Token = token
					}

					return info, nil
				}
			}
		}
	}

	return info, nil
}

// getCurrentUserToken gets the current user's token for API access
func (c *Client) getCurrentUserToken(ctx context.Context) (string, error) {
	// Try to read token from service account
	tokenPath := "/var/run/secrets/kubernetes.io/serviceaccount/token"
	if _, err := os.Stat(tokenPath); err == nil {
		tokenBytes, err := os.ReadFile(tokenPath)
		if err == nil {
			return string(tokenBytes), nil
		}
	}

	// If running outside cluster, try to get token from kubeconfig
	if c.config.BearerToken != "" {
		return c.config.BearerToken, nil
	}

	return "", fmt.Errorf("no authentication token available")
}
