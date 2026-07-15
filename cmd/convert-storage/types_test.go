package convert_storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGenerateSuffix(t *testing.T) {
	s, err := GenerateSuffix()
	if err != nil {
		t.Fatalf("GenerateSuffix() error: %v", err)
	}
	if len(s) != suffixLength {
		t.Errorf("suffix length = %d, want %d", len(s), suffixLength)
	}
	for _, c := range s {
		if !strings.ContainsRune(suffixCharset, c) {
			t.Errorf("suffix contains invalid char %q", c)
		}
	}
	s2, _ := GenerateSuffix()
	if s == s2 {
		t.Errorf("two consecutive suffixes should differ (got %q both times)", s)
	}
}

func TestGenerateTargetName(t *testing.T) {
	tests := []struct {
		name     string
		source   string
		suffix   string
		expected string
	}{
		{
			name:     "normal name",
			source:   "mysql-data",
			suffix:   "a1b2",
			expected: "mysql-data-mig-a1b2",
		},
		{
			name:     "empty suffix",
			source:   "data",
			suffix:   "",
			expected: "data-mig-",
		},
		{
			name:   "very long name truncated",
			source: strings.Repeat("a", 300),
			suffix: "x9y8",
			expected: func() string {
				maxBase := maxK8sNameLen - len(migInfix) - suffixLength
				return strings.Repeat("a", maxBase) + "-mig-x9y8"
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateTargetName(tt.source, tt.suffix)
			if got != tt.expected {
				t.Errorf("GenerateTargetName(%q, %q) = %q, want %q", tt.source, tt.suffix, got, tt.expected)
			}
			if len(got) > maxK8sNameLen {
				t.Errorf("result exceeds %d chars: len=%d", maxK8sNameLen, len(got))
			}
		})
	}
}

func TestValidatePlan(t *testing.T) {
	tests := []struct {
		name    string
		plan    *ConversionPlan
		wantErr string
	}{
		{
			name: "valid plan",
			plan: &ConversionPlan{
				Context: "mycluster", Namespace: "myapp", Suffix: "a1b2",
				PVCs: []PVCEntry{{Name: "pvc1", TargetStorageClass: "gp3", TargetName: "pvc1-mig-a1b2", Action: "convert"}},
			},
		},
		{
			name:    "missing context",
			plan:    &ConversionPlan{Namespace: "ns", Suffix: "a1b2", PVCs: []PVCEntry{{Name: "pvc1", Action: "skip"}}},
			wantErr: "context is required",
		},
		{
			name:    "missing namespace",
			plan:    &ConversionPlan{Context: "ctx", Suffix: "a1b2", PVCs: []PVCEntry{{Name: "pvc1", Action: "skip"}}},
			wantErr: "namespace is required",
		},
		{
			name:    "no PVCs",
			plan:    &ConversionPlan{Context: "ctx", Namespace: "ns", Suffix: "a1b2", PVCs: []PVCEntry{}},
			wantErr: "at least one PVC entry",
		},
		{
			name: "invalid action",
			plan: &ConversionPlan{
				Context: "ctx", Namespace: "ns", Suffix: "a1b2",
				PVCs: []PVCEntry{{Name: "pvc1", Action: "move"}},
			},
			wantErr: "must be 'convert' or 'skip'",
		},
		{
			name: "convert without target SC",
			plan: &ConversionPlan{
				Context: "ctx", Namespace: "ns", Suffix: "a1b2",
				PVCs: []PVCEntry{{Name: "pvc1", TargetName: "pvc1-mig-a1b2", Action: "convert"}},
			},
			wantErr: "targetStorageClass is required",
		},
		{
			name: "skip entry is valid without target",
			plan: &ConversionPlan{
				Context: "ctx", Namespace: "ns", Suffix: "a1b2",
				PVCs: []PVCEntry{{Name: "pvc1", Action: "skip"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePlan(tt.plan)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("ValidatePlan() unexpected error: %v", err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("ValidatePlan() error = %v, want containing %q", err, tt.wantErr)
				}
			}
		})
	}
}

func TestPlanRoundTrip(t *testing.T) {
	plan := &ConversionPlan{
		Context:   "mycluster",
		Namespace: "myapp",
		Suffix:    "x7k9",
		Endpoint:  "route",
		PVCs: []PVCEntry{
			{Name: "mysql-data", SourceStorageClass: "gp2", TargetStorageClass: "gp3", TargetName: "mysql-data-mig-x7k9", Capacity: "10Gi", AccessModes: []string{"ReadWriteOnce"}, Action: "convert"},
			{Name: "cache", SourceStorageClass: "gp2", Action: "skip"},
		},
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.yaml")
	if err := WritePlan(plan, path); err != nil {
		t.Fatalf("WritePlan() error: %v", err)
	}
	data, _ := os.ReadFile(path)
	if len(data) == 0 {
		t.Fatal("plan file is empty")
	}
	loaded, err := LoadPlan(path)
	if err != nil {
		t.Fatalf("LoadPlan() error: %v", err)
	}
	if loaded.Context != plan.Context {
		t.Errorf("Context = %q, want %q", loaded.Context, plan.Context)
	}
	if loaded.Suffix != plan.Suffix {
		t.Errorf("Suffix = %q, want %q", loaded.Suffix, plan.Suffix)
	}
	if len(loaded.PVCs) != len(plan.PVCs) {
		t.Fatalf("PVCs count = %d, want %d", len(loaded.PVCs), len(plan.PVCs))
	}
	if loaded.PVCs[0].TargetStorageClass != "gp3" {
		t.Errorf("PVCs[0].TargetStorageClass = %q, want %q", loaded.PVCs[0].TargetStorageClass, "gp3")
	}
	if loaded.PVCs[1].Action != "skip" {
		t.Errorf("PVCs[1].Action = %q, want %q", loaded.PVCs[1].Action, "skip")
	}
}

func TestSuggestTargetSC(t *testing.T) {
	scGP2 := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "gp2"}, Provisioner: "kubernetes.io/aws-ebs"}
	scGP3 := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "gp3", Annotations: map[string]string{"storageclass.kubernetes.io/is-default-class": "true"}}, Provisioner: "ebs.csi.aws.com"}
	scEFS := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "efs-sc"}, Provisioner: "efs.csi.aws.com"}
	scGluster := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "glusterfs"}, Provisioner: "kubernetes.io/glusterfs"}
	scCeph := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "cephfs"}, Provisioner: "cephfs.csi.ceph.com"}
	scNFS := storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: "nfs"}, Provisioner: "nfs-subdir-external-provisioner"}

	tests := []struct {
		name     string
		source   string
		allSCs   []storagev1.StorageClass
		expected string
	}{
		{
			name:     "default SC when no provisioner match",
			source:   "gp2",
			allSCs:   []storagev1.StorageClass{scGP2, scGP3, scEFS},
			expected: "gp3",
		},
		{
			name:     "gluster to ceph",
			source:   "glusterfs",
			allSCs:   []storagev1.StorageClass{scGluster, scCeph, scGP3},
			expected: "cephfs",
		},
		{
			name:     "nfs to ceph",
			source:   "nfs",
			allSCs:   []storagev1.StorageClass{scNFS, scCeph, scGP3},
			expected: "cephfs",
		},
		{
			name:     "no suggestion when source is the only SC besides default which is same",
			source:   "gp3",
			allSCs:   []storagev1.StorageClass{scGP3},
			expected: "",
		},
		{
			name:     "falls back to default when source has unique provisioner",
			source:   "efs-sc",
			allSCs:   []storagev1.StorageClass{scEFS, scGP3, scGP2},
			expected: "gp3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byProv := make(map[string][]string)
			var defaultSC string
			for _, sc := range tt.allSCs {
				byProv[sc.Provisioner] = append(byProv[sc.Provisioner], sc.Name)
				if sc.Annotations["storageclass.kubernetes.io/is-default-class"] == "true" {
					defaultSC = sc.Name
				}
			}
			got := suggestTargetSC(tt.source, tt.allSCs, byProv, defaultSC)
			if got != tt.expected {
				t.Errorf("suggestTargetSC(%q) = %q, want %q", tt.source, got, tt.expected)
			}
		})
	}
}

func TestBuildPlanFromPVCs(t *testing.T) {
	scName := "gp2"
	pvcs := []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-data", Namespace: "myapp"},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "redis-data", Namespace: "myapp"},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &scName,
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
				},
			},
		},
	}

	plan := BuildPlanFromPVCs(pvcs, "mycluster", "myapp", "x7k9")

	if plan.Context != "mycluster" {
		t.Errorf("Context = %q, want %q", plan.Context, "mycluster")
	}
	if plan.Suffix != "x7k9" {
		t.Errorf("Suffix = %q, want %q", plan.Suffix, "x7k9")
	}
	if len(plan.PVCs) != 2 {
		t.Fatalf("PVCs count = %d, want 2", len(plan.PVCs))
	}
	if plan.PVCs[0].Name != "mysql-data" {
		t.Errorf("PVCs[0].Name = %q, want %q", plan.PVCs[0].Name, "mysql-data")
	}
	if plan.PVCs[0].TargetName != "mysql-data-mig-x7k9" {
		t.Errorf("PVCs[0].TargetName = %q, want %q", plan.PVCs[0].TargetName, "mysql-data-mig-x7k9")
	}
	if plan.PVCs[0].SourceStorageClass != "gp2" {
		t.Errorf("PVCs[0].SourceStorageClass = %q, want %q", plan.PVCs[0].SourceStorageClass, "gp2")
	}
	if plan.PVCs[0].Capacity != "10Gi" {
		t.Errorf("PVCs[0].Capacity = %q, want %q", plan.PVCs[0].Capacity, "10Gi")
	}
	if plan.PVCs[0].Action != "convert" {
		t.Errorf("PVCs[0].Action = %q, want %q", plan.PVCs[0].Action, "convert")
	}
	if plan.PVCs[1].TargetName != "redis-data-mig-x7k9" {
		t.Errorf("PVCs[1].TargetName = %q, want %q", plan.PVCs[1].TargetName, "redis-data-mig-x7k9")
	}
}
