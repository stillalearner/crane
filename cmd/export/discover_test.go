package export

import (
	"fmt"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

func makeCRD(name, group, plural string) unstructured.Unstructured {
	return unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": name,
			},
			"spec": map[string]interface{}{
				"group": group,
				"names": map[string]interface{}{
					"plural": plural,
					"kind":   "Any",
				},
				"scope": "Namespaced",
				"versions": []interface{}{
					map[string]interface{}{"name": "v1", "served": true, "storage": true},
				},
			},
		},
	}
}

func TestDeriveCandidateCRDKeys(t *testing.T) {
	resources := []*groupResource{
		{
			APIGroup: "example.com",
			APIResource: metav1.APIResource{
				Name:       "widgets",
				Namespaced: true,
			},
		},
		{
			APIGroup: "example.com",
			APIResource: metav1.APIResource{
				Name:       "widgets",
				Namespaced: true,
			},
		},
		{
			APIGroup: "",
			APIResource: metav1.APIResource{
				Name:       "configmaps",
				Namespaced: true,
			},
		},
		{
			APIGroup: "rbac.authorization.k8s.io",
			APIResource: metav1.APIResource{
				Name:       "clusterroles",
				Namespaced: false,
			},
		},
	}

	got := deriveCandidateCRDKeys(resources)
	if len(got) != 1 {
		t.Fatalf("expected 1 candidate key, got %d", len(got))
	}
	if _, ok := got["widgets.example.com"]; !ok {
		t.Fatalf("expected widgets.example.com candidate")
	}
}

func TestCollectRelatedCRDs_Intersection(t *testing.T) {
	scheme := runtime.NewScheme()
	crdMatch := makeCRD("widgets.example.com", "example.com", "widgets")
	crdOther := makeCRD("gadgets.other.io", "other.io", "gadgets")
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{
			{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
		},
		&crdMatch, &crdOther,
	)

	resources := []*groupResource{
		{
			APIGroup: "example.com",
			APIResource: metav1.APIResource{
				Name:       "widgets",
				Namespaced: true,
			},
		},
	}

	got, errEntry := collectRelatedCRDs(resources, client)
	if errEntry != nil {
		t.Fatalf("unexpected error entry: %#v", errEntry)
	}
	if got == nil {
		t.Fatalf("expected matched CRD resource, got nil")
	}
	if got.APIResource.Name != "customresourcedefinitions" {
		t.Fatalf("unexpected APIResource name: %s", got.APIResource.Name)
	}
	if got.objects == nil || len(got.objects.Items) != 1 {
		t.Fatalf("expected exactly 1 matched CRD, got %d", len(got.objects.Items))
	}
	if got.objects.Items[0].GetName() != "widgets.example.com" {
		t.Fatalf("unexpected CRD matched: %s", got.objects.Items[0].GetName())
	}
}

func TestCollectRelatedCRDs_ListForbiddenRecorded(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
		},
	)
	client.PrependReactor("list", "customresourcedefinitions", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"},
			"",
			fmt.Errorf("forbidden"),
		)
	})

	resources := []*groupResource{
		{
			APIGroup: "example.com",
			APIResource: metav1.APIResource{
				Name:       "widgets",
				Namespaced: true,
			},
		},
	}

	got, errEntry := collectRelatedCRDs(resources, client)
	if got != nil {
		t.Fatalf("expected no CRD resource on forbidden list")
	}
	if errEntry == nil {
		t.Fatalf("expected error entry for forbidden CRD list")
	}
	if errEntry.APIResource.Name != "customresourcedefinitions" {
		t.Fatalf("unexpected error entry APIResource: %s", errEntry.APIResource.Name)
	}
}
