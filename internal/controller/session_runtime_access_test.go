package controller

import (
	"context"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kelos "github.com/kelos-dev/kelos/api/v1alpha2"
)

func TestEnsureSessionRuntimeAccess(t *testing.T) {
	tests := []struct {
		name               string
		serviceAccountName string
		managed            bool
	}{
		{name: "managed service account", managed: true},
		{name: "configured service account", serviceAccountName: "workload-identity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = rbacv1.AddToScheme(scheme)
			_ = kelos.AddToScheme(scheme)
			session := testSession("chat", "codex")
			serviceAccountName := tt.serviceAccountName
			if tt.serviceAccountName != "" {
				session.Spec.Worker.PodOverrides = &kelos.PodOverrides{ServiceAccountName: tt.serviceAccountName}
			} else {
				serviceAccountName = sessionRuntimeAccessName(session)
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session).Build()
			reconciler := &SessionReconciler{Client: cl, Scheme: scheme}
			if err := reconciler.ensureSessionRuntimeAccess(context.Background(), session, serviceAccountName, nil); err != nil {
				t.Fatal(err)
			}

			key := client.ObjectKey{Namespace: session.Namespace, Name: sessionRuntimeAccessName(session)}
			var serviceAccount corev1.ServiceAccount
			err := cl.Get(context.Background(), key, &serviceAccount)
			if tt.managed {
				if err != nil || !metav1.IsControlledBy(&serviceAccount, session) {
					t.Fatalf("managed ServiceAccount error = %v, object = %#v", err, serviceAccount)
				}
			} else if !apierrors.IsNotFound(err) {
				t.Fatalf("configured ServiceAccount get error = %v, want NotFound", err)
			}

			var role rbacv1.Role
			if err := cl.Get(context.Background(), key, &role); err != nil {
				t.Fatal(err)
			}
			wantRules := []rbacv1.PolicyRule{
				{
					APIGroups:     []string{kelos.GroupVersion.Group},
					Resources:     []string{"sessions"},
					ResourceNames: []string{session.Name},
					Verbs:         []string{"get", "watch", "patch"},
				},
				{
					APIGroups:     []string{kelos.GroupVersion.Group},
					Resources:     []string{"sessions/status"},
					ResourceNames: []string{session.Name},
					Verbs:         []string{"patch"},
				},
				{
					APIGroups: []string{kelos.GroupVersion.Group},
					Resources: []string{"sessionmessages"},
					Verbs:     []string{"get", "list", "watch"},
				},
				{
					APIGroups: []string{kelos.GroupVersion.Group},
					Resources: []string{"sessionmessages/status"},
					Verbs:     []string{"patch", "update"},
				},
			}
			if !reflect.DeepEqual(role.Rules, wantRules) {
				t.Fatalf("Session runtime Role rules = %#v, want %#v", role.Rules, wantRules)
			}

			var roleBinding rbacv1.RoleBinding
			if err := cl.Get(context.Background(), key, &roleBinding); err != nil {
				t.Fatal(err)
			}
			if len(roleBinding.Subjects) != 1 || roleBinding.Subjects[0].Name != serviceAccountName || roleBinding.RoleRef.Name != key.Name {
				t.Fatalf("Session runtime RoleBinding = %#v", roleBinding)
			}
		})
	}
}

func TestSessionRuntimeAccessServiceAccountTransition(t *testing.T) {
	for _, tt := range []struct {
		name string
		from string
		to   string
	}{
		{name: "managed to configured", to: "configured"},
		{name: "configured to managed", from: "configured"},
		{name: "configured to configured", from: "first", to: "second"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{appsv1.AddToScheme, corev1.AddToScheme, rbacv1.AddToScheme, kelos.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			session := testSession("chat", "codex")
			from, to := tt.from, tt.to
			if from == "" {
				from = sessionRuntimeAccessName(session)
			}
			if to == "" {
				to = sessionRuntimeAccessName(session)
			}
			statefulSet := testSessionStatefulSet(session)
			statefulSet.Spec.Template.Spec.ServiceAccountName = from
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: statefulSet.Name + "-0", Namespace: session.Namespace, UID: types.UID("running-pod"),
					OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
				},
				Spec: corev1.PodSpec{ServiceAccountName: from},
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(session, statefulSet, pod).Build()
			reconciler := &SessionReconciler{Client: cl, Scheme: scheme}
			key := client.ObjectKey{Namespace: session.Namespace, Name: sessionRuntimeAccessName(session)}
			checkAccess := func(desired string, accounts ...string) {
				t.Helper()
				session.Spec.Worker.PodOverrides = nil
				if desired != sessionRuntimeAccessName(session) {
					session.Spec.Worker.PodOverrides = &kelos.PodOverrides{ServiceAccountName: desired}
				}
				if err := reconciler.ensureSessionRuntimeAccess(ctx, session, desired, statefulSet); err != nil {
					t.Fatal(err)
				}
				var binding rbacv1.RoleBinding
				if err := cl.Get(ctx, key, &binding); err != nil {
					t.Fatal(err)
				}
				want := make([]rbacv1.Subject, 0, len(accounts))
				for _, account := range accounts {
					want = append(want, rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: session.Namespace, Name: account})
				}
				if !reflect.DeepEqual(binding.Subjects, want) {
					t.Fatalf("runtime subjects = %#v, want %#v", binding.Subjects, want)
				}
			}

			checkAccess(from, from)
			checkAccess(to, to, from)
			checkAccess(to, to, from)
			checkAccess("intermediate", "intermediate", from)
			checkAccess(to, to, from)
			if err := cl.Delete(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}); err != nil {
				t.Fatal(err)
			}
			checkAccess(to, to, from)

			pod.Finalizers = []string{"test.kelos.dev/hold"}
			if err := cl.Update(ctx, pod); err != nil {
				t.Fatal(err)
			}
			if err := cl.Delete(ctx, pod); err != nil {
				t.Fatal(err)
			}
			checkAccess(to, to, from)
			if err := cl.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
				t.Fatal(err)
			}
			pod.Finalizers = nil
			if err := cl.Update(ctx, pod); err != nil {
				t.Fatal(err)
			}
			checkAccess(to, to, from)
			if err := cl.Delete(ctx, &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}); err != nil {
				t.Fatal(err)
			}
			checkAccess(to, to, from)

			pod.ResourceVersion = ""
			pod.DeletionTimestamp = nil
			pod.UID = types.UID("replacement-pod")
			pod.Spec.ServiceAccountName = to
			if err := cl.Create(ctx, pod); err != nil {
				t.Fatal(err)
			}
			checkAccess(to, to)
			if tt.to == "" {
				if err := cl.Get(ctx, key, &corev1.ServiceAccount{}); err != nil {
					t.Fatalf("getting managed service account: %v", err)
				}
			}
		})
	}
}
