package sessionruntime

import (
	"context"
	"encoding/json"
	"fmt"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	kelos "github.com/kelos-dev/kelos/api/v1alpha2"
	clientv1alpha2 "github.com/kelos-dev/kelos/pkg/generated/clientset/versioned/typed/api/v1alpha2"
)

// ObservedSessionStatus contains the status fields owned by one Session runtime.
type ObservedSessionStatus struct {
	Active          bool
	WaitingForInput bool
	Model           string
	// WorkspaceStatus is omitted when the runtime cannot inspect the workspace.
	WorkspaceStatus *WorkspaceStatus
}

// SessionStatusPublisher publishes runtime-observed state for one Session Pod.
type SessionStatusPublisher func(context.Context, ObservedSessionStatus) error

type sessionStatusPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// NewSessionStatusPublisher creates a publisher scoped to one Session Pod.
func NewSessionStatusPublisher(client clientv1alpha2.SessionInterface, sessionName string, podUID types.UID) (SessionStatusPublisher, error) {
	if client == nil || sessionName == "" || podUID == "" {
		return nil, fmt.Errorf("Session client, name, and Pod UID are required")
	}
	return func(ctx context.Context, status ObservedSessionStatus) error {
		session, err := client.Get(ctx, sessionName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("getting Session %q before publishing runtime status: %w", sessionName, err)
		}
		if session.Status.Phase != kelos.SessionPhaseReady || session.Status.PodUID != podUID {
			return fmt.Errorf("Session %q is %q on Pod %q, not Ready on Pod %q; runtime status not published yet",
				sessionName, session.Status.Phase, session.Status.PodUID, podUID)
		}
		conditionStatus := metav1.ConditionFalse
		reason := "Idle"
		message := "Session runtime is idle"
		if status.Active {
			conditionStatus = metav1.ConditionTrue
			reason = "TurnActive"
			message = "Session runtime has unfinished work"
			if status.WaitingForInput {
				reason = "WaitingForInput"
				message = "Session runtime is waiting for user input"
			}
		}
		previousActive := apiMeta.FindStatusCondition(session.Status.Conditions, kelos.SessionConditionActive)
		conditions := append([]metav1.Condition(nil), session.Status.Conditions...)
		apiMeta.SetStatusCondition(&conditions, metav1.Condition{
			Type:               kelos.SessionConditionActive,
			Status:             conditionStatus,
			ObservedGeneration: session.Generation,
			Reason:             reason,
			Message:            message,
		})
		activityTime := session.Status.LastActivityTime
		activeStateChanged := previousActive != nil && previousActive.Status != metav1.ConditionUnknown &&
			(previousActive.Status != conditionStatus || previousActive.Reason != reason)
		if activityTime == nil || activeStateChanged {
			if previousActive != nil && previousActive.Status != metav1.ConditionUnknown && previousActive.Status == conditionStatus {
				if previousActive.Reason == reason {
					activityTime = &previousActive.LastTransitionTime
				} else {
					now := metav1.Now()
					activityTime = &now
				}
			} else if active := apiMeta.FindStatusCondition(conditions, kelos.SessionConditionActive); active != nil {
				activityTime = &active.LastTransitionTime
			}
		}
		operations := make([]sessionStatusPatchOperation, 0, 8)
		if session.ResourceVersion != "" {
			operations = append(operations, sessionStatusPatchOperation{Op: "test", Path: "/metadata/resourceVersion", Value: session.ResourceVersion})
		}
		operations = append(operations,
			sessionStatusPatchOperation{Op: "test", Path: "/status/podUID", Value: string(podUID)},
			sessionStatusPatchOperation{Op: "test", Path: "/status/phase", Value: kelos.SessionPhaseReady},
			sessionStatusPatchOperation{Op: "add", Path: "/status/conditions", Value: conditions},
			sessionStatusPatchOperation{Op: "add", Path: "/status/lastActivityTime", Value: activityTime},
			sessionStatusPatchOperation{Op: "add", Path: "/status/model", Value: status.Model},
		)
		if status.WorkspaceStatus != nil {
			operations = append(operations,
				sessionStatusPatchOperation{Op: "add", Path: "/status/branch", Value: status.WorkspaceStatus.Branch},
				sessionStatusPatchOperation{Op: "add", Path: "/status/pullRequest", Value: status.WorkspaceStatus.PullRequest},
			)
		}
		patch, err := json.Marshal(operations)
		if err != nil {
			return fmt.Errorf("encoding Session runtime status patch: %w", err)
		}
		if _, err := client.Patch(ctx, sessionName, types.JSONPatchType, patch, metav1.PatchOptions{}, "status"); err != nil {
			return fmt.Errorf("publishing Session %q runtime status: %w%s", sessionName, err,
				concurrentWrite(ctx, client, session))
		}
		return nil
	}, nil
}

// concurrentWrite: a failed JSON-patch test op is a bare 422 (no op named), so re-read to tell a race.
func concurrentWrite(ctx context.Context, client clientv1alpha2.SessionInterface, read *kelos.Session) string {
	latest, err := client.Get(ctx, read.Name, metav1.GetOptions{})
	if err != nil || latest.ResourceVersion == read.ResourceVersion {
		return ""
	}
	return fmt.Sprintf(" (Session changed from resourceVersion %s to %s while publishing; retrying)",
		read.ResourceVersion, latest.ResourceVersion)
}
