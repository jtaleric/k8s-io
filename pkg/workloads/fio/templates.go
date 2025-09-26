package fio

import (
	"context"
	"embed"
	"fmt"
	"regexp"
	"strings"

	"github.com/flosch/pongo2/v6"
	"github.com/jtaleric/k8s-io/pkg/config"
	"github.com/jtaleric/k8s-io/pkg/kubernetes"
)

//go:embed templates/*.j2
var embeddedTemplates embed.FS

// safeElasticsearch returns a safe Elasticsearch config or empty map if nil
func safeElasticsearch(es *config.ElasticsearchConfig) interface{} {
	if es == nil {
		return map[string]interface{}{}
	}
	// Convert struct to map for better template compatibility
	return map[string]interface{}{
		"url":         es.URL,
		"index_name":  es.IndexName,
		"verify_cert": es.VerifyCert,
		"parallel":    es.Parallel,
	}
}

// safePrometheus returns a safe Prometheus config or empty map if nil
func safePrometheus(prom interface{}) interface{} {
	if prom == nil {
		return map[string]interface{}{}
	}
	return prom
}

// TemplateEngine handles FIO template processing
type TemplateEngine struct {
	templateSet *pongo2.TemplateSet
}

// NewTemplateEngine creates a new FIO template engine
func NewTemplateEngine(templatesDir string) *TemplateEngine {
	// Note: templatesDir parameter is kept for backward compatibility but not used
	// Templates are now embedded in the binary
	templateSet := pongo2.NewSet("fio-templates", nil)

	return &TemplateEngine{
		templateSet: templateSet,
	}
}

// LoadTemplate loads and preprocesses a template file
func (e *TemplateEngine) LoadTemplate(templatePath string) (*pongo2.Template, error) {
	// Read from embedded filesystem
	content, err := embeddedTemplates.ReadFile("templates/" + templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded template file %s: %w", templatePath, err)
	}

	// Preprocess Jinja2 syntax to Pongo2 compatible syntax
	processedContent := e.preprocessJinja2ToPongo2(string(content))

	// Create template from processed content
	template, err := e.templateSet.FromString(processedContent)
	if err != nil {
		return nil, fmt.Errorf("failed to compile template %s: %w", templatePath, err)
	}

	return template, nil
}

// RenderTemplate renders a template with the given context
func (e *TemplateEngine) RenderTemplate(templatePath string, context pongo2.Context) (string, error) {
	template, err := e.LoadTemplate(templatePath)
	if err != nil {
		return "", fmt.Errorf("failed to load template %s: %w", templatePath, err)
	}

	rendered, err := template.Execute(context)
	if err != nil {
		return "", fmt.Errorf("failed to render template %s: %w", templatePath, err)
	}

	return rendered, nil
}

// preprocessJinja2ToPongo2 converts Jinja2 specific syntax to Pongo2 compatible syntax
func (e *TemplateEngine) preprocessJinja2ToPongo2(content string) string {
	// Handle "is defined" checks
	isDefined := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+defined`)
	content = isDefined.ReplaceAllString(content, "$1")

	// Handle "is sameas true/false" checks
	isSameasTrue := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+sameas\s+true`)
	content = isSameasTrue.ReplaceAllString(content, "$1")

	isSameasFalse := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+sameas\s+false`)
	content = isSameasFalse.ReplaceAllString(content, "not $1")

	// Handle "is defined and/or" patterns
	isDefinedAnd := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+defined\s+and\s+`)
	content = isDefinedAnd.ReplaceAllString(content, "$1 and ")

	isDefinedOr := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+defined\s+or\s+`)
	content = isDefinedOr.ReplaceAllString(content, "$1 or ")

	// Handle "is mapping" checks
	isMapping := regexp.MustCompile(`(\w+(?:\.\w+)*)\s+is\s+mapping`)
	content = isMapping.ReplaceAllString(content, "$1")

	// Handle |default([])| length pattern - convert to simple existence check
	defaultLength := regexp.MustCompile(`(\w+(?:\.\w+)*)\|default\(\[\]\)\|length`)
	content = defaultLength.ReplaceAllString(content, "$1")

	// Handle |default() without parameters
	defaultFilterEmpty := regexp.MustCompile(`\|\s*default\(\)`)
	content = defaultFilterEmpty.ReplaceAllString(content, `|default:""`)

	// Handle |default(value) with parameters
	defaultFilter := regexp.MustCompile(`\|\s*default\(([^)]+)\)`)
	content = defaultFilter.ReplaceAllString(content, `|default:$1`)

	// Handle |replace filter (remove it as Pongo2 doesn't have direct equivalent)
	replaceFilter := regexp.MustCompile(`\|\s*replace\s*\([^)]+\)`)
	content = replaceFilter.ReplaceAllString(content, "")

	// Handle .items() method calls
	itemsMethod := regexp.MustCompile(`\.items\(\)`)
	content = itemsMethod.ReplaceAllString(content, "")

	// Handle |json_query filter
	jsonQuery := regexp.MustCompile(`\|\s*json_query\(['"]([^'"]+)['"]\)`)
	content = jsonQuery.ReplaceAllString(content, ".$1")

	// Handle |selectattr filter (remove it)
	selectAttr := regexp.MustCompile(`\|\s*selectattr\([^)]+\)`)
	content = selectAttr.ReplaceAllString(content, "")

	// Handle |list filter (remove it)
	listFilter := regexp.MustCompile(`\|\s*list`)
	content = listFilter.ReplaceAllString(content, "")

	// Handle |length filter
	lengthFilter := regexp.MustCompile(`\|\s*length`)
	content = lengthFilter.ReplaceAllString(content, "|len")

	return content
}

// createBaseContext creates the base context for template rendering
func (e *TemplateEngine) createBaseContext(cfg *config.Config) pongo2.Context {
	return pongo2.Context{
		"uuid":                        cfg.UUID,
		"trunc_uuid":                  cfg.GetTruncatedUUID(),
		"test_user":                   cfg.TestUser,
		"clustername":                 cfg.ClusterName,
		"namespace":                   cfg.Namespace,
		"kcache_drop_pod_ips":         cfg.KCacheDropPodIPs,
		"kernel_cache_drop_svc_port":  cfg.KernelCacheDropSvcPort,
		"ceph_osd_cache_drop_pod_ip":  cfg.CephOSDCacheDropPodIP,
		"ceph_cache_drop_svc_port":    cfg.CephCacheDropSvcPort,
		"rook_ceph_drop_cache_pod_ip": cfg.RookCephDropCachePodIP,
		"elasticsearch":               safeElasticsearch(cfg.Elasticsearch),
		"prometheus":                  safePrometheus(nil), // Will be set by createContextWithPrometheus
	}
}

// createContextWithPrometheus creates context with discovered Prometheus info
func (e *TemplateEngine) createContextWithPrometheus(cfg *config.Config, k8sClient interface{}) pongo2.Context {
	templateContext := e.createBaseContext(cfg)

	// Handle Prometheus configuration (user-provided or auto-discovered)
	var prometheusContext pongo2.Context

	// Check if user provided Prometheus configuration directly
	if cfg.Prometheus != nil && cfg.Prometheus.URL != "" {
		prometheusContext = pongo2.Context{
			"url":        cfg.Prometheus.URL,
			"prom_token": cfg.Prometheus.Token,
		}
	} else if client, ok := k8sClient.(*kubernetes.Client); ok {
		// Try to auto-discover Prometheus
		ctx := context.Background()
		if promInfo, err := client.DiscoverPrometheusWithConfig(ctx, cfg.Prometheus); err == nil && promInfo.Found {
			prometheusContext = pongo2.Context{
				"url":        promInfo.URL,
				"prom_token": promInfo.Token,
			}
		}
	}

	// If we have Prometheus context, add Elasticsearch configuration and set it
	if prometheusContext != nil {
		// Add Elasticsearch configuration if available
		if cfg.Elasticsearch != nil && cfg.Elasticsearch.URL != "" {
			prometheusContext["es_url"] = cfg.Elasticsearch.URL
			prometheusContext["es_parallel"] = cfg.Elasticsearch.Parallel
		}

		templateContext["prometheus"] = prometheusContext
	}

	return templateContext
}

// RenderFIOConfigMap renders the FIO configuration map
func (e *TemplateEngine) RenderFIOConfigMap(cfg *config.Config, fioConfig *FIOConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["fio_path"] = fioConfig.GetFIOPath()
	context["job_params"] = cfg.JobParams

	return e.RenderTemplate("configmap.yml.j2", context)
}

// RenderFIOPrefillConfigMap renders the FIO prefill configuration map
func (e *TemplateEngine) RenderFIOPrefillConfigMap(cfg *config.Config, fioConfig *FIOConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["fio_path"] = fioConfig.GetFIOPath()

	return e.RenderTemplate("prefill-configmap.yml.j2", context)
}

// RenderFIOServer renders a FIO server pod
func (e *TemplateEngine) RenderFIOServer(cfg *config.Config, fioConfig *FIOConfig, serverNum int) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["server_num"] = serverNum
	context["fio_path"] = fioConfig.GetFIOPath()

	return e.RenderTemplate("servers.yaml.j2", context)
}

// RenderFIOServerVM renders a FIO server VM
func (e *TemplateEngine) RenderFIOServerVM(cfg *config.Config, fioConfig *FIOConfig, serverNum int) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["server_num"] = serverNum
	context["resource_kind"] = "vm"
	context["fio_path"] = fioConfig.GetFIOPath()

	return e.RenderTemplate("server_vm.yml.j2", context)
}

// RenderFIOClient renders the FIO client job
func (e *TemplateEngine) RenderFIOClient(cfg *config.Config, fioConfig *FIOConfig, podDetails map[string]string) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["pod_details"] = podDetails

	return e.RenderTemplate("client.yaml.j2", context)
}

// RenderFIOClientWithPrometheus renders the FIO client job with auto-discovered Prometheus
func (e *TemplateEngine) RenderFIOClientWithPrometheus(cfg *config.Config, fioConfig *FIOConfig, podDetails map[string]string, k8sClient interface{}) (string, error) {
	context := e.createContextWithPrometheus(cfg, k8sClient)
	context["workload_args"] = fioConfig
	context["pod_details"] = podDetails

	return e.RenderTemplate("client.yaml.j2", context)
}

// RenderFIOPrefillClient renders the FIO prefill client job
func (e *TemplateEngine) RenderFIOPrefillClient(cfg *config.Config, fioConfig *FIOConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig

	return e.RenderTemplate("prefill-client.yaml.j2", context)
}

// RenderFIOPVC renders a FIO PVC
func (e *TemplateEngine) RenderFIOPVC(cfg *config.Config, fioConfig *FIOConfig, serverNum int) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = fioConfig
	context["server_num"] = serverNum

	pvcTemplate := `---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fio-claim-{{ server_num }}-{{ trunc_uuid }}
  namespace: '{{ namespace }}'
  labels:
    app: "fio-benchmark-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
spec:
  accessModes:
    - "{{ workload_args.PVCAccessMode }}"
  volumeMode: "{{ workload_args.PVCVolumeMode }}"
  resources:
    requests:
      storage: "{{ workload_args.StorageSize }}"
{% if workload_args.StorageClass %}
  storageClassName: "{{ workload_args.StorageClass }}"
{% endif %}`

	template, err := e.templateSet.FromString(pvcTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to compile PVC template: %w", err)
	}

	return template.Execute(context)
}

// RenderHostsConfigMap renders the hosts configuration map
func (e *TemplateEngine) RenderHostsConfigMap(cfg *config.Config, hosts []string) (string, error) {
	var indentedHosts []string
	for _, host := range hosts {
		indentedHosts = append(indentedHosts, "    "+host) // Indent each host line
	}
	hostsData := strings.Join(indentedHosts, "\n")

	template := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: fio-hosts-{{ trunc_uuid }}
  namespace: '{{ namespace }}'
  labels:
    app: "fio-benchmark-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
data:
  hosts: |
{{ hosts_data }}`

	context := e.createBaseContext(cfg)
	context["hosts_data"] = hostsData

	tmpl, err := e.templateSet.FromString(template)
	if err != nil {
		return "", fmt.Errorf("failed to compile hosts configmap template: %w", err)
	}

	return tmpl.Execute(context)
}
