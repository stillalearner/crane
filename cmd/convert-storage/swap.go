package convert_storage

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// QuiesceWorkloadsForPVC scales down all workloads in the namespace that
// reference the given PVC name. Returns a map of workload name -> original
// replica count so they can be restored after conversion.
func QuiesceWorkloadsForPVC(c client.Client, namespace, pvcName string) (map[string]int32, error) {
	origReplicas := make(map[string]int32)
	zero := int32(0)

	var deployments appsv1.DeploymentList
	if err := c.List(context.TODO(), &deployments, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing deployments: %w", err)
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if !podSpecReferencesPVC(d.Spec.Template.Spec, pvcName) {
			continue
		}
		if d.Spec.Replicas != nil && *d.Spec.Replicas > 0 {
			origReplicas[fmt.Sprintf("Deployment/%s", d.Name)] = *d.Spec.Replicas
			d.Spec.Replicas = &zero
			if err := c.Update(context.TODO(), d); err != nil {
				return origReplicas, fmt.Errorf("scaling Deployment %s to 0: %w", d.Name, err)
			}
			log.Printf("  Quiesced Deployment/%s (was %d replicas)", d.Name, origReplicas[fmt.Sprintf("Deployment/%s", d.Name)])
		}
	}

	var statefulSets appsv1.StatefulSetList
	if err := c.List(context.TODO(), &statefulSets, client.InNamespace(namespace)); err != nil {
		return origReplicas, fmt.Errorf("listing statefulsets: %w", err)
	}
	for i := range statefulSets.Items {
		ss := &statefulSets.Items[i]
		refs := podSpecReferencesPVC(ss.Spec.Template.Spec, pvcName)
		if !refs {
			for _, vct := range ss.Spec.VolumeClaimTemplates {
				pattern := regexp.MustCompile(
					fmt.Sprintf(`^%s-%s-\d+$`, regexp.QuoteMeta(vct.Name), regexp.QuoteMeta(ss.Name)))
				if pattern.MatchString(pvcName) {
					refs = true
					break
				}
			}
		}
		if refs && ss.Spec.Replicas != nil && *ss.Spec.Replicas > 0 {
			origReplicas[fmt.Sprintf("StatefulSet/%s", ss.Name)] = *ss.Spec.Replicas
			ss.Spec.Replicas = &zero
			if err := c.Update(context.TODO(), ss); err != nil {
				return origReplicas, fmt.Errorf("scaling StatefulSet %s to 0: %w", ss.Name, err)
			}
			log.Printf("  Quiesced StatefulSet/%s (was %d replicas)", ss.Name, origReplicas[fmt.Sprintf("StatefulSet/%s", ss.Name)])
		}
	}

	var daemonSets appsv1.DaemonSetList
	if err := c.List(context.TODO(), &daemonSets, client.InNamespace(namespace)); err != nil {
		return origReplicas, fmt.Errorf("listing daemonsets: %w", err)
	}
	for i := range daemonSets.Items {
		ds := &daemonSets.Items[i]
		if podSpecReferencesPVC(ds.Spec.Template.Spec, pvcName) {
			log.Printf("  WARN: DaemonSet/%s references PVC %s — cannot scale DaemonSets to 0, data may be inconsistent", ds.Name, pvcName)
		}
	}

	return origReplicas, nil
}

// RestoreWorkloadReplicas restores workloads to their original replica counts
// after quiesce + swap. Should be called after SwapWorkloadPVCReferences.
func RestoreWorkloadReplicas(c client.Client, namespace string, origReplicas map[string]int32) error {
	for key, replicas := range origReplicas {
		r := replicas
		parts := regexp.MustCompile(`^(\w+)/(.+)$`).FindStringSubmatch(key)
		if len(parts) != 3 {
			continue
		}
		kind, name := parts[1], parts[2]
		switch kind {
		case "Deployment":
			var updated bool
			for attempt := 0; attempt < 5; attempt++ {
				d := &appsv1.Deployment{}
				if err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: name}, d); err != nil {
					log.Printf("  WARN: cannot restore %s: %v", key, err)
					break
				}
				d.Spec.Replicas = &r
				if err := c.Update(context.TODO(), d); err != nil {
					continue
				}
				updated = true
				break
			}
			if updated {
				log.Printf("  Restored %s to %d replicas", key, replicas)
			} else {
				log.Printf("  WARN: failed to restore %s after retries", key)
			}
		case "StatefulSet":
			// StatefulSet replicas are restored by the delete+recreate dance
			log.Printf("  StatefulSet %s replicas handled by template swap", name)
		}
	}
	return nil
}

// SwapWorkloadPVCReferences finds all workloads in the namespace that reference
// oldPVCName and patches them to reference newPVCName. For StatefulSets with
// matching volumeClaimTemplates, it performs the delete+recreate dance required
// by the immutable template field. targetSC is applied to the new template's
// storageClassName. Returns a list of updated workload descriptions.
func SwapWorkloadPVCReferences(c client.Client, namespace, oldPVCName, newPVCName, targetSC string) ([]string, error) {
	var swapped []string

	// Deployments
	var deployments appsv1.DeploymentList
	if err := c.List(context.TODO(), &deployments, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing deployments: %w", err)
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if swapVolumesInPodSpec(&d.Spec.Template.Spec, oldPVCName, newPVCName) {
			if err := c.Update(context.TODO(), d); err != nil {
				return swapped, fmt.Errorf("updating Deployment %s: %w", d.Name, err)
			}
			swapped = append(swapped, fmt.Sprintf("Deployment/%s", d.Name))
		}
	}

	// DaemonSets
	var daemonSets appsv1.DaemonSetList
	if err := c.List(context.TODO(), &daemonSets, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing daemonsets: %w", err)
	}
	for i := range daemonSets.Items {
		ds := &daemonSets.Items[i]
		if swapVolumesInPodSpec(&ds.Spec.Template.Spec, oldPVCName, newPVCName) {
			if err := c.Update(context.TODO(), ds); err != nil {
				return swapped, fmt.Errorf("updating DaemonSet %s: %w", ds.Name, err)
			}
			swapped = append(swapped, fmt.Sprintf("DaemonSet/%s", ds.Name))
		}
	}

	// ReplicaSets (standalone only — skip owned by Deployments)
	var replicaSets appsv1.ReplicaSetList
	if err := c.List(context.TODO(), &replicaSets, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing replicasets: %w", err)
	}
	for i := range replicaSets.Items {
		rs := &replicaSets.Items[i]
		if len(rs.OwnerReferences) > 0 {
			continue
		}
		if swapVolumesInPodSpec(&rs.Spec.Template.Spec, oldPVCName, newPVCName) {
			if err := c.Update(context.TODO(), rs); err != nil {
				return swapped, fmt.Errorf("updating ReplicaSet %s: %w", rs.Name, err)
			}
			swapped = append(swapped, fmt.Sprintf("ReplicaSet/%s", rs.Name))
		}
	}

	// CronJobs
	var cronJobs batchv1.CronJobList
	if err := c.List(context.TODO(), &cronJobs, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing cronjobs: %w", err)
	}
	for i := range cronJobs.Items {
		cj := &cronJobs.Items[i]
		if swapVolumesInPodSpec(&cj.Spec.JobTemplate.Spec.Template.Spec, oldPVCName, newPVCName) {
			if err := c.Update(context.TODO(), cj); err != nil {
				return swapped, fmt.Errorf("updating CronJob %s: %w", cj.Name, err)
			}
			swapped = append(swapped, fmt.Sprintf("CronJob/%s", cj.Name))
		}
	}

	// Jobs — immutable, must delete + recreate. Skip completed Jobs.
	var jobs batchv1.JobList
	if err := c.List(context.TODO(), &jobs, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing jobs: %w", err)
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !podSpecReferencesPVC(j.Spec.Template.Spec, oldPVCName) {
			continue
		}
		if j.Status.Succeeded > 0 || j.Status.CompletionTime != nil {
			log.Printf("  Skipping completed Job/%s (already finished)", j.Name)
			continue
		}
		swapVolumesInPodSpec(&j.Spec.Template.Spec, oldPVCName, newPVCName)
		newJob := j.DeepCopy()
		newJob.ResourceVersion = ""
		newJob.UID = ""
		newJob.Status = batchv1.JobStatus{}
		if newJob.Labels == nil {
			newJob.Labels = make(map[string]string)
		}
		newJob.Spec.Selector = nil
		newJob.Spec.Template.Labels = nil

		if err := c.Delete(context.TODO(), j, client.PropagationPolicy("Background")); err != nil {
			log.Printf("WARN: failed to delete Job %s for recreation: %v", j.Name, err)
			continue
		}
		if err := c.Create(context.TODO(), newJob); err != nil {
			return swapped, fmt.Errorf("recreating Job %s: %w", j.Name, err)
		}
		swapped = append(swapped, fmt.Sprintf("Job/%s (recreated)", j.Name))
	}

	// StatefulSets — two cases:
	// 1. Pod spec volumes referencing oldPVCName directly -> patch
	// 2. volumeClaimTemplates matching oldPVCName -> delete+recreate
	var statefulSets appsv1.StatefulSetList
	if err := c.List(context.TODO(), &statefulSets, client.InNamespace(namespace)); err != nil {
		return swapped, fmt.Errorf("listing statefulsets: %w", err)
	}
	for idx := range statefulSets.Items {
		ss := &statefulSets.Items[idx]

		// Check if a volumeClaimTemplate matches
		templateIdx := -1
		for i, vct := range ss.Spec.VolumeClaimTemplates {
			if vct.Name == oldPVCName {
				templateIdx = i
				break
			}
		}

		if templateIdx >= 0 {
			// Delete+recreate dance for immutable volumeClaimTemplates
			result, err := swapStatefulSetTemplate(c, ss, templateIdx, oldPVCName, newPVCName, targetSC)
			if err != nil {
				return swapped, err
			}
			swapped = append(swapped, result)
			continue
		}

		// Simple pod spec volume swap (no template match)
		if swapVolumesInPodSpec(&ss.Spec.Template.Spec, oldPVCName, newPVCName) {
			if err := c.Update(context.TODO(), ss); err != nil {
				return swapped, fmt.Errorf("updating StatefulSet %s: %w", ss.Name, err)
			}
			swapped = append(swapped, fmt.Sprintf("StatefulSet/%s", ss.Name))
		}
	}

	return swapped, nil
}

func swapStatefulSetTemplate(c client.Client, ss *appsv1.StatefulSet, templateIdx int, oldName, newName, targetSC string) (string, error) {
	originalReplicas := int32(1)
	if ss.Spec.Replicas != nil {
		originalReplicas = *ss.Spec.Replicas
	}

	// Step 1: Scale to 0
	zero := int32(0)
	ss.Spec.Replicas = &zero
	if err := c.Update(context.TODO(), ss); err != nil {
		return "", fmt.Errorf("scaling StatefulSet %s to 0: %w", ss.Name, err)
	}
	log.Printf("Scaled StatefulSet %s to 0 replicas", ss.Name)

	// Step 2: Create temporary StatefulSet to hold label selector
	tempSS := ss.DeepCopy()
	tempSS.Name = ss.Name + "-mig-tmp"
	tempSS.ResourceVersion = ""
	tempSS.UID = ""
	tempSS.Spec.Replicas = &zero
	if tempSS.Annotations == nil {
		tempSS.Annotations = make(map[string]string)
	}
	tempSS.Annotations["crane.konveyor.io/temporary"] = "true"

	if err := c.Create(context.TODO(), tempSS); err != nil {
		log.Printf("WARN: failed to create temporary StatefulSet %s: %v (proceeding anyway)", tempSS.Name, err)
	}

	// Step 3: Modify spec — rename template + update volumeMounts
	ss.Spec.VolumeClaimTemplates[templateIdx].Name = newName
	if targetSC != "" {
		ss.Spec.VolumeClaimTemplates[templateIdx].Spec.StorageClassName = &targetSC
	}
	updateContainerVolumeMounts(ss.Spec.Template.Spec.Containers, oldName, newName)
	updateContainerVolumeMounts(ss.Spec.Template.Spec.InitContainers, oldName, newName)
	swapVolumesInPodSpec(&ss.Spec.Template.Spec, oldName, newName)

	// Step 4: Delete original StatefulSet
	if err := c.Delete(context.TODO(), ss); err != nil {
		return "", fmt.Errorf("deleting StatefulSet %s for recreation: %w", ss.Name, err)
	}
	log.Printf("Deleted StatefulSet %s", ss.Name)

	// Step 5: Recreate with modified spec
	newSS := ss.DeepCopy()
	newSS.ResourceVersion = ""
	newSS.UID = ""
	newSS.Status = appsv1.StatefulSetStatus{}
	newSS.Spec.Replicas = &originalReplicas
	if err := c.Create(context.TODO(), newSS); err != nil {
		return "", fmt.Errorf("recreating StatefulSet %s: %w", ss.Name, err)
	}
	log.Printf("Recreated StatefulSet %s with template %s -> %s (replicas: %d)", ss.Name, oldName, newName, originalReplicas)

	// Step 6: Delete temporary StatefulSet
	if err := c.Delete(context.TODO(), tempSS); err != nil {
		log.Printf("WARN: failed to delete temporary StatefulSet %s: %v", tempSS.Name, err)
	}

	return fmt.Sprintf("StatefulSet/%s (recreated, template: %s->%s)", ss.Name, oldName, newName), nil
}

func updateContainerVolumeMounts(containers []corev1.Container, oldName, newName string) {
	for i := range containers {
		for j := range containers[i].VolumeMounts {
			if containers[i].VolumeMounts[j].Name == oldName {
				containers[i].VolumeMounts[j].Name = newName
			}
		}
	}
}

func swapVolumesInPodSpec(spec *corev1.PodSpec, oldName, newName string) bool {
	swapped := false
	for i := range spec.Volumes {
		if spec.Volumes[i].PersistentVolumeClaim != nil &&
			spec.Volumes[i].PersistentVolumeClaim.ClaimName == oldName {
			spec.Volumes[i].PersistentVolumeClaim.ClaimName = newName
			swapped = true
		}
	}
	return swapped
}

func podSpecReferencesPVC(spec corev1.PodSpec, pvcName string) bool {
	for _, vol := range spec.Volumes {
		if vol.PersistentVolumeClaim != nil && vol.PersistentVolumeClaim.ClaimName == pvcName {
			return true
		}
	}
	return false
}

// DetectAndSwapStatefulSetTemplates inspects converted PVC name mappings and
// automatically detects which ones belong to StatefulSet volumeClaimTemplates.
// For each detected StatefulSet template, it performs the delete+recreate dance.
//
// PVC naming convention: <templateName>-<statefulSetName>-<ordinal>
// e.g., "data-redis-0" → template "data", StatefulSet "redis", ordinal 0
func DetectAndSwapStatefulSetTemplates(c client.Client, namespace string, pvcMapping map[string]string, targetSC string) ([]string, error) {
	var swapped []string

	var statefulSets appsv1.StatefulSetList
	if err := c.List(context.TODO(), &statefulSets, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("listing statefulsets: %w", err)
	}

	if len(statefulSets.Items) == 0 {
		return nil, nil
	}

	// For each StatefulSet, check if any converted PVCs match its template pattern
	type templateSwap struct {
		oldTemplateName string
		newTemplateName string
		targetSC        string
	}

	for idx := range statefulSets.Items {
		ss := &statefulSets.Items[idx]

		for _, vct := range ss.Spec.VolumeClaimTemplates {
			// Build regex: ^<templateName>-<stsName>-(\d+)$
			pattern := regexp.MustCompile(
				fmt.Sprintf(`^%s-%s-(\d+)$`, regexp.QuoteMeta(vct.Name), regexp.QuoteMeta(ss.Name)))

			var swap *templateSwap
			for oldPVC, newPVC := range pvcMapping {
				matches := pattern.FindStringSubmatch(oldPVC)
				if matches == nil {
					continue
				}
				// Verify ordinal is a valid number
				if _, err := strconv.Atoi(matches[1]); err != nil {
					continue
				}
				// Extract new template name from new PVC using same pattern
				newPattern := regexp.MustCompile(
					fmt.Sprintf(`^(.+)-%s-\d+$`, regexp.QuoteMeta(ss.Name)))
				newMatches := newPattern.FindStringSubmatch(newPVC)
				if newMatches == nil {
					continue
				}
				swap = &templateSwap{
					oldTemplateName: vct.Name,
					newTemplateName: newMatches[1],
					targetSC:        targetSC,
				}
				break
			}

			if swap == nil {
				continue
			}

			// Re-fetch the StatefulSet (it may have been modified)
			currentSS := &appsv1.StatefulSet{}
			if err := c.Get(context.TODO(), client.ObjectKey{Namespace: namespace, Name: ss.Name}, currentSS); err != nil {
				return swapped, fmt.Errorf("re-fetching StatefulSet %s: %w", ss.Name, err)
			}

			templateIdx := -1
			for i, t := range currentSS.Spec.VolumeClaimTemplates {
				if t.Name == swap.oldTemplateName {
					templateIdx = i
					break
				}
			}
			if templateIdx == -1 {
				continue
			}

			result, err := swapStatefulSetTemplate(c, currentSS, templateIdx, swap.oldTemplateName, swap.newTemplateName, swap.targetSC)
			if err != nil {
				return swapped, err
			}
			swapped = append(swapped, result)
		}
	}

	return swapped, nil
}
