package convert_storage

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
)

const (
	suffixCharset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffixLength  = 4
	maxK8sNameLen = 253
	migInfix      = "-mig-"

	labelMigratedBy = "crane.konveyor.io/migrated-by"
	labelMigratedTo = "crane.konveyor.io/migrated-to"
)

type ConversionPlan struct {
	Context   string     `yaml:"context"`
	Namespace string     `yaml:"namespace"`
	Suffix    string     `yaml:"suffix"`
	Endpoint  string     `yaml:"endpoint,omitempty"`
	Subdomain string     `yaml:"subdomain,omitempty"`
	PVCs      []PVCEntry `yaml:"pvcs"`
}

type PVCEntry struct {
	Name               string   `yaml:"name"`
	SourceStorageClass string   `yaml:"sourceStorageClass"`
	TargetStorageClass string   `yaml:"targetStorageClass"`
	TargetName         string   `yaml:"targetName"`
	Capacity           string   `yaml:"capacity"`
	AccessModes        []string `yaml:"accessModes"`
	Action             string   `yaml:"action"`
}

func LoadPlan(path string) (*ConversionPlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read plan file %q: %w", path, err)
	}
	plan := &ConversionPlan{}
	if err := yaml.Unmarshal(data, plan); err != nil {
		return nil, fmt.Errorf("failed to parse plan file %q: %w", path, err)
	}
	if err := ValidatePlan(plan); err != nil {
		return nil, fmt.Errorf("invalid plan file %q: %w", path, err)
	}
	return plan, nil
}

func WritePlan(plan *ConversionPlan, path string) error {
	data, err := yaml.Marshal(plan)
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write plan file %q: %w", path, err)
	}
	return nil
}

func ValidatePlan(plan *ConversionPlan) error {
	if plan.Context == "" {
		return fmt.Errorf("context is required")
	}
	if plan.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if plan.Suffix == "" {
		return fmt.Errorf("suffix is required")
	}
	if len(plan.PVCs) == 0 {
		return fmt.Errorf("at least one PVC entry is required")
	}
	for i, pvc := range plan.PVCs {
		if pvc.Name == "" {
			return fmt.Errorf("pvcs[%d].name is required", i)
		}
		if pvc.Action != "convert" && pvc.Action != "skip" {
			return fmt.Errorf("pvcs[%d].action must be 'convert' or 'skip', got %q", i, pvc.Action)
		}
		if pvc.Action == "convert" {
			if pvc.TargetStorageClass == "" {
				return fmt.Errorf("pvcs[%d].targetStorageClass is required when action is 'convert'", i)
			}
			if pvc.TargetName == "" {
				return fmt.Errorf("pvcs[%d].targetName is required when action is 'convert'", i)
			}
		}
	}
	return nil
}

func GenerateSuffix() (string, error) {
	var sb strings.Builder
	for i := 0; i < suffixLength; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(suffixCharset))))
		if err != nil {
			return "", fmt.Errorf("failed to generate random suffix: %w", err)
		}
		sb.WriteByte(suffixCharset[n.Int64()])
	}
	return sb.String(), nil
}

func GenerateTargetName(sourceName, suffix string) string {
	base := sourceName
	maxBase := maxK8sNameLen - len(migInfix) - suffixLength
	if len(base) > maxBase {
		base = base[:maxBase]
	}
	return base + migInfix + suffix
}

func BuildPlanFromPVCs(pvcs []corev1.PersistentVolumeClaim, context, namespace, suffix string) *ConversionPlan {
	plan := &ConversionPlan{
		Context:   context,
		Namespace: namespace,
		Suffix:    suffix,
		PVCs:      make([]PVCEntry, 0, len(pvcs)),
	}
	for _, pvc := range pvcs {
		sc := ""
		if pvc.Spec.StorageClassName != nil {
			sc = *pvc.Spec.StorageClassName
		}
		accessModes := make([]string, 0, len(pvc.Spec.AccessModes))
		for _, am := range pvc.Spec.AccessModes {
			accessModes = append(accessModes, string(am))
		}
		capacity := ""
		if q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
			capacity = q.String()
		}
		plan.PVCs = append(plan.PVCs, PVCEntry{
			Name:               pvc.Name,
			SourceStorageClass: sc,
			TargetStorageClass: "",
			TargetName:         GenerateTargetName(pvc.Name, suffix),
			Capacity:           capacity,
			AccessModes:        accessModes,
			Action:             "convert",
		})
	}
	return plan
}
