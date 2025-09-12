package hammerdb

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"

	"github.com/flosch/pongo2/v6"
	"github.com/jtaleric/k8s-io/pkg/config"
)

// TemplateEngine handles HammerDB template processing
type TemplateEngine struct {
	templatesDir string
	templateSet  *pongo2.TemplateSet
}

// NewTemplateEngine creates a new HammerDB template engine
func NewTemplateEngine(templatesDir string) *TemplateEngine {
	templateSet := pongo2.NewSet("hammerdb-templates", pongo2.MustNewLocalFileSystemLoader(templatesDir))

	return &TemplateEngine{
		templatesDir: templatesDir,
		templateSet:  templateSet,
	}
}

// LoadTemplate loads and preprocesses a template file
func (e *TemplateEngine) LoadTemplate(templatePath string) (*pongo2.Template, error) {
	fullPath := filepath.Join(e.templatesDir, templatePath)
	content, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file %s: %w", fullPath, err)
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

	// Handle |default([])| length pattern
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
		"workload_name":               "hammerdb",
		"kcache_drop_pod_ips":         cfg.KCacheDropPodIPs,
		"kernel_cache_drop_svc_port":  cfg.KernelCacheDropSvcPort,
		"ceph_osd_cache_drop_pod_ip":  cfg.CephOSDCacheDropPodIP,
		"ceph_cache_drop_svc_port":    cfg.CephCacheDropSvcPort,
		"rook_ceph_drop_cache_pod_ip": cfg.RookCephDropCachePodIP,
		"elasticsearch":               cfg.Elasticsearch,
		"prometheus":                  cfg.Prometheus,
	}
}

// RenderHammerDBPVC renders a HammerDB PVC
func (e *TemplateEngine) RenderHammerDBPVC(cfg *config.Config, hammerdbConfig *HammerDBConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig

	pvcTemplate := `---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: "claim-{{ trunc_uuid }}"
  namespace: '{{ namespace }}'
  labels:
    app: "hammerdb-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
  annotations:
    volume.beta.kubernetes.io/storage-class: "{{ workload_args.ClientVM.PVCStorageClass }}"
spec:
  accessModes:
    - "{{ workload_args.ClientVM.PVCAccessMode }}"
  volumeMode: "{{ workload_args.ClientVM.PVCVolumeMode }}"
  resources:
    requests:
      storage: "{{ workload_args.ClientVM.PVCStorageSize }}"`

	template, err := e.templateSet.FromString(pvcTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to compile HammerDB PVC template: %w", err)
	}

	return template.Execute(context)
}

// RenderHammerDBCreateScript renders the HammerDB database creation script configmap
func (e *TemplateEngine) RenderHammerDBCreateScript(cfg *config.Config, hammerdbConfig *HammerDBConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig

	var templateFile string
	switch hammerdbConfig.DBType {
	case "mariadb":
		templateFile = "createdb_mariadb.tcl.j2"
	case "mssql":
		templateFile = "createdb_mssql.tcl.j2"
	case "pg":
		templateFile = "createdb_pg.tcl.j2"
	default:
		return "", fmt.Errorf("unsupported database type: %s", hammerdbConfig.DBType)
	}

	scriptContent, err := e.RenderTemplate(templateFile, context)
	if err != nil {
		return "", err
	}

	configMapTemplate := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: 'hammerdb-creator-{{ trunc_uuid }}'
  namespace: '{{ namespace }}'
  labels:
    app: "hammerdb-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
data:
  createdb.tcl: |
{{ script_content | indent:"    " }}`

	context["script_content"] = scriptContent

	template, err := e.templateSet.FromString(configMapTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to compile create script configmap template: %w", err)
	}

	return template.Execute(context)
}

// RenderHammerDBWorkloadScript renders the HammerDB workload script configmap
func (e *TemplateEngine) RenderHammerDBWorkloadScript(cfg *config.Config, hammerdbConfig *HammerDBConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig

	var templateFile string
	switch hammerdbConfig.DBType {
	case "mariadb":
		templateFile = "tpcc-workload-mariadb.tcl.j2"
	case "mssql":
		templateFile = "tpcc-workload-mssql.tcl.j2"
	case "pg":
		templateFile = "tpcc-workload-pg.tcl.j2"
	default:
		return "", fmt.Errorf("unsupported database type: %s", hammerdbConfig.DBType)
	}

	scriptContent, err := e.RenderTemplate(templateFile, context)
	if err != nil {
		return "", err
	}

	var scriptKey string
	switch hammerdbConfig.DBType {
	case "mariadb":
		scriptKey = "tpcc-workload-mariadb.tcl"
	case "mssql":
		scriptKey = "tpcc-workload-mssql.tcl"
	case "pg":
		scriptKey = "tpcc-workload-pg.tcl"
	}

	configMapTemplate := fmt.Sprintf(`---
apiVersion: v1
kind: ConfigMap
metadata:
  name: 'hammerdb-workload-{{ trunc_uuid }}'
  namespace: '{{ namespace }}'
  labels:
    app: "hammerdb-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
data:
  %s: |
{{ script_content | indent:"    " }}`, scriptKey)

	context["script_content"] = scriptContent

	template, err := e.templateSet.FromString(configMapTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to compile workload script configmap template: %w", err)
	}

	return template.Execute(context)
}

// RenderHammerDBVMWorkloadScript renders the HammerDB VM workload script configmap
func (e *TemplateEngine) RenderHammerDBVMWorkloadScript(cfg *config.Config, hammerdbConfig *HammerDBConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig

	var templateFile, scriptKey, configMapName string
	switch hammerdbConfig.DBType {
	case "mariadb":
		templateFile = "db_mariadb_workload_vm.sh.j2"
		scriptKey = "run_mariadb_script.sh"
		configMapName = "hammerdb-mariadb-workload"
	case "mssql":
		templateFile = "db_mssql_workload_vm.sh.j2"
		scriptKey = "run_mssql_script.sh"
		configMapName = "hammerdb-mssql-workload"
	case "pg":
		templateFile = "db_postgres_workload_vm.sh.j2"
		scriptKey = "run_postgres_script.sh"
		configMapName = "hammerdb-postgres-workload"
	default:
		return "", fmt.Errorf("unsupported database type: %s", hammerdbConfig.DBType)
	}

	scriptContent, err := e.RenderTemplate(templateFile, context)
	if err != nil {
		return "", err
	}

	configMapTemplate := fmt.Sprintf(`---
apiVersion: v1
kind: ConfigMap
metadata:
  name: '%s-{{ trunc_uuid }}'
  namespace: '{{ namespace }}'
  labels:
    app: "hammerdb-{{ trunc_uuid }}"
    benchmark-uuid: "{{ uuid }}"
data:
  %s: |
{{ script_content | indent:"    " }}`, configMapName, scriptKey)

	context["script_content"] = scriptContent

	template, err := e.templateSet.FromString(configMapTemplate)
	if err != nil {
		return "", fmt.Errorf("failed to compile VM workload script configmap template: %w", err)
	}

	return template.Execute(context)
}

// RenderHammerDBCreateJob renders the HammerDB database creation job
func (e *TemplateEngine) RenderHammerDBCreateJob(cfg *config.Config, hammerdbConfig *HammerDBConfig) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig
	context["resource_kind"] = hammerdbConfig.Kind

	return e.RenderTemplate("db_creation.yml", context)
}

// RenderHammerDBCreateJobVM renders the HammerDB database creation job for VMs
func (e *TemplateEngine) RenderHammerDBCreateJobVM(cfg *config.Config, hammerdbConfig *HammerDBConfig, dbType string) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig
	context["resource_kind"] = "vm"

	templateFile := fmt.Sprintf("db_creation_%s_vm.yml", dbType)
	return e.RenderTemplate(templateFile, context)
}

// RenderHammerDBWorkloadJob renders the HammerDB workload job
func (e *TemplateEngine) RenderHammerDBWorkloadJob(cfg *config.Config, hammerdbConfig *HammerDBConfig, dbType string) (string, error) {
	context := e.createBaseContext(cfg)
	context["workload_args"] = hammerdbConfig
	context["resource_kind"] = hammerdbConfig.Kind

	templateFile := fmt.Sprintf("db_%s_workload.yml.j2", dbType)
	return e.RenderTemplate(templateFile, context)
}
