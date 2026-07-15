package convert_storage

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = batchv1.AddToScheme(s)
	return s
}

func pvcVolume(name, claimName string) corev1.Volume {
	return corev1.Volume{
		Name: name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
			},
		},
	}
}

func TestSwapWorkloadPVCReferences_Deployment(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "myapp"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					Volumes:    []corev1.Volume{pvcVolume("data", "mysql-data")},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(deploy).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "mysql-data", "mysql-data-mig-x7k9", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 {
		t.Fatalf("swapped count = %d, want 1", len(swapped))
	}
	if swapped[0] != "Deployment/web" {
		t.Errorf("swapped[0] = %q, want %q", swapped[0], "Deployment/web")
	}

	updated := &appsv1.Deployment{}
	if err := c.Get(context.TODO(), client.ObjectKey{Namespace: "myapp", Name: "web"}, updated); err != nil {
		t.Fatal(err)
	}
	claimName := updated.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
	if claimName != "mysql-data-mig-x7k9" {
		t.Errorf("volume claim = %q, want %q", claimName, "mysql-data-mig-x7k9")
	}
}

func TestSwapWorkloadPVCReferences_NoMatch(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "myapp"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					Volumes:    []corev1.Volume{pvcVolume("data", "other-pvc")},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(deploy).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "mysql-data", "mysql-data-mig-x7k9", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 0 {
		t.Errorf("swapped count = %d, want 0 (no match)", len(swapped))
	}
}

func TestSwapWorkloadPVCReferences_DaemonSet(t *testing.T) {
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: "logger", Namespace: "myapp"},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "logger"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "logger"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "log", Image: "fluentd"}},
					Volumes:    []corev1.Volume{pvcVolume("logs", "log-data")},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(ds).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "log-data", "log-data-mig-ab12", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 || swapped[0] != "DaemonSet/logger" {
		t.Errorf("swapped = %v, want [DaemonSet/logger]", swapped)
	}
}

func TestSwapWorkloadPVCReferences_ReplicaSet_Standalone(t *testing.T) {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-rs", Namespace: "myapp"},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "rs"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "rs"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					Volumes:    []corev1.Volume{pvcVolume("data", "rs-data")},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(rs).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "rs-data", "rs-data-mig-zz99", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 || swapped[0] != "ReplicaSet/standalone-rs" {
		t.Errorf("swapped = %v, want [ReplicaSet/standalone-rs]", swapped)
	}
}

func TestSwapWorkloadPVCReferences_ReplicaSet_Owned(t *testing.T) {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: "owned-rs", Namespace: "myapp",
			OwnerReferences: []metav1.OwnerReference{{Name: "deploy", Kind: "Deployment", APIVersion: "apps/v1", UID: "abc"}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "rs"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "rs"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					Volumes:    []corev1.Volume{pvcVolume("data", "rs-data")},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(rs).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "rs-data", "rs-data-mig-zz99", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 0 {
		t.Errorf("swapped = %v, want [] (owned RS should be skipped)", swapped)
	}
}

func TestSwapWorkloadPVCReferences_CronJob(t *testing.T) {
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "myapp"},
		Spec: batchv1.CronJobSpec{
			Schedule: "*/5 * * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers:    []corev1.Container{{Name: "bak", Image: "busybox"}},
							RestartPolicy: corev1.RestartPolicyNever,
							Volumes:       []corev1.Volume{pvcVolume("backup-vol", "backup-data")},
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(cj).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "backup-data", "backup-data-mig-1234", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 || swapped[0] != "CronJob/backup" {
		t.Errorf("swapped = %v, want [CronJob/backup]", swapped)
	}

	updated := &batchv1.CronJob{}
	if err := c.Get(context.TODO(), client.ObjectKey{Namespace: "myapp", Name: "backup"}, updated); err != nil {
		t.Fatal(err)
	}
	claimName := updated.Spec.JobTemplate.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName
	if claimName != "backup-data-mig-1234" {
		t.Errorf("cronjob volume claim = %q, want %q", claimName, "backup-data-mig-1234")
	}
}

func TestSwapWorkloadPVCReferences_MultipleWorkloads(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "myapp"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
					Volumes:    []corev1.Volume{pvcVolume("data", "shared-pvc")},
				},
			},
		},
	}
	cj := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "myapp"},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 * * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers:    []corev1.Container{{Name: "c", Image: "busybox"}},
							RestartPolicy: corev1.RestartPolicyNever,
							Volumes:       []corev1.Volume{pvcVolume("vol", "shared-pvc")},
						},
					},
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(deploy, cj).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "shared-pvc", "shared-pvc-mig-abcd", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 2 {
		t.Errorf("swapped count = %d, want 2", len(swapped))
	}
}

func TestSwapWorkloadPVCReferences_StatefulSet(t *testing.T) {
	replicas := int32(3)
	scName := "gp2"
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql", Namespace: "myapp"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mysql"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mysql"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "mysql",
						Image: "mysql:8",
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/var/lib/mysql"},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &scName,
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(ss).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "data", "data-mig-x7k9", "gp3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 {
		t.Fatalf("swapped count = %d, want 1", len(swapped))
	}

	// Verify recreated StatefulSet
	recreated := &appsv1.StatefulSet{}
	if err := c.Get(context.TODO(), client.ObjectKey{Namespace: "myapp", Name: "mysql"}, recreated); err != nil {
		t.Fatalf("StatefulSet not found after recreate: %v", err)
	}

	// Check template name renamed
	if recreated.Spec.VolumeClaimTemplates[0].Name != "data-mig-x7k9" {
		t.Errorf("template name = %q, want %q", recreated.Spec.VolumeClaimTemplates[0].Name, "data-mig-x7k9")
	}

	// Check storageClassName updated
	if *recreated.Spec.VolumeClaimTemplates[0].Spec.StorageClassName != "gp3" {
		t.Errorf("template SC = %q, want %q", *recreated.Spec.VolumeClaimTemplates[0].Spec.StorageClassName, "gp3")
	}

	// Check volumeMount renamed
	if recreated.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name != "data-mig-x7k9" {
		t.Errorf("volumeMount name = %q, want %q", recreated.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name, "data-mig-x7k9")
	}

	// Check replicas restored
	if *recreated.Spec.Replicas != 3 {
		t.Errorf("replicas = %d, want 3", *recreated.Spec.Replicas)
	}

	// Verify temporary SS was cleaned up
	tempSS := &appsv1.StatefulSet{}
	err = c.Get(context.TODO(), client.ObjectKey{Namespace: "myapp", Name: "mysql-mig-tmp"}, tempSS)
	if err == nil {
		t.Error("temporary StatefulSet should have been deleted")
	}
}

func TestSwapWorkloadPVCReferences_StatefulSet_NoMatch(t *testing.T) {
	replicas := int32(1)
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "myapp"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis"}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "redis", Image: "redis"}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "redis-data"},
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(ss).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "nonexistent-template", "new-name", "gp3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 0 {
		t.Errorf("swapped = %v, want [] (no matching template)", swapped)
	}
}

func TestSwapWorkloadPVCReferences_StatefulSet_WithInitContainers(t *testing.T) {
	replicas := int32(2)
	scName := "gp2"
	ss := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "myapp"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "app"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "app"}},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{{
						Name:         "init",
						Image:        "busybox",
						VolumeMounts: []corev1.VolumeMount{{Name: "storage", MountPath: "/init-data"}},
					}},
					Containers: []corev1.Container{{
						Name:         "main",
						Image:        "nginx",
						VolumeMounts: []corev1.VolumeMount{{Name: "storage", MountPath: "/data"}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "storage"},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &scName,
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			}},
		},
	}

	c := fake.NewClientBuilder().WithScheme(testScheme()).WithRuntimeObjects(ss).Build()
	swapped, err := SwapWorkloadPVCReferences(c, "myapp", "storage", "storage-mig-ab12", "gp3")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(swapped) != 1 {
		t.Fatalf("swapped count = %d, want 1", len(swapped))
	}

	recreated := &appsv1.StatefulSet{}
	if err := c.Get(context.TODO(), client.ObjectKey{Namespace: "myapp", Name: "app"}, recreated); err != nil {
		t.Fatal(err)
	}

	// Both init and main container volumeMounts should be renamed
	if recreated.Spec.Template.Spec.InitContainers[0].VolumeMounts[0].Name != "storage-mig-ab12" {
		t.Errorf("init volumeMount = %q, want %q", recreated.Spec.Template.Spec.InitContainers[0].VolumeMounts[0].Name, "storage-mig-ab12")
	}
	if recreated.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name != "storage-mig-ab12" {
		t.Errorf("main volumeMount = %q, want %q", recreated.Spec.Template.Spec.Containers[0].VolumeMounts[0].Name, "storage-mig-ab12")
	}
}
