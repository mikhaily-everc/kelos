package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// SessionMessageType identifies what a SessionMessage asks the Session runtime to do.
type SessionMessageType string

const (
	// SessionMessageTypeMessage submits text as a user message.
	SessionMessageTypeMessage SessionMessageType = "message"
	// SessionMessageTypeInput answers or cancels a pending input request.
	SessionMessageTypeInput SessionMessageType = "input"
	// SessionMessageTypeInterrupt interrupts the active turn.
	SessionMessageTypeInterrupt SessionMessageType = "interrupt"
	// SessionMessageTypeRemove removes a pending turn that has not started yet.
	SessionMessageTypeRemove SessionMessageType = "remove"
)

// SessionMessagePhase represents the delivery and outcome of a SessionMessage.
type SessionMessagePhase string

const (
	// SessionMessagePhasePending means the message has not been delivered yet.
	SessionMessagePhasePending SessionMessagePhase = "Pending"
	// SessionMessagePhaseAccepted means the runtime accepted the message and
	// queued it for a turn that has not started yet.
	SessionMessagePhaseAccepted SessionMessagePhase = "Accepted"
	// SessionMessagePhaseWorking means the turn carrying the message is running.
	SessionMessagePhaseWorking SessionMessagePhase = "Working"
	// SessionMessagePhaseInputRequired means the turn is waiting for an answer.
	SessionMessagePhaseInputRequired SessionMessagePhase = "InputRequired"
	// SessionMessagePhaseCompleted means the message was fully processed.
	SessionMessagePhaseCompleted SessionMessagePhase = "Completed"
	// SessionMessagePhaseFailed means the turn carrying the message failed.
	SessionMessagePhaseFailed SessionMessagePhase = "Failed"
	// SessionMessagePhaseCanceled means the turn was interrupted or removed.
	SessionMessagePhaseCanceled SessionMessagePhase = "Canceled"
	// SessionMessagePhaseRejected means the message could not be delivered.
	SessionMessagePhaseRejected SessionMessagePhase = "Rejected"
)

// Reasons reported in SessionMessage status.
const (
	// SessionMessageReasonPodLost means the Session Pod or runtime processing the
	// message stopped before its turn finished.
	SessionMessageReasonPodLost = "PodLost"
	// SessionMessageReasonTurnLost means the runtime no longer records the turn.
	SessionMessageReasonTurnLost = "TurnLost"
	// SessionMessageReasonTurnFailed means the provider turn failed.
	SessionMessageReasonTurnFailed = "TurnFailed"
	// SessionMessageReasonInterrupted means the turn was interrupted.
	SessionMessageReasonInterrupted = "Interrupted"
	// SessionMessageReasonRemoved means the message was removed before its turn started.
	SessionMessageReasonRemoved = "Removed"
	// SessionMessageReasonInvalidRequest means the runtime refused the request.
	SessionMessageReasonInvalidRequest = "InvalidRequest"
	// SessionMessageReasonTaskNotFound means spec.taskRef does not name a message
	// for the same Session.
	SessionMessageReasonTaskNotFound = "TaskNotFound"
	// SessionMessageReasonNotPending means the referenced message already started.
	SessionMessageReasonNotPending = "NotPending"
	// SessionMessageReasonNoActiveTurn means there is no matching running turn.
	SessionMessageReasonNoActiveTurn = "NoActiveTurn"
	// SessionMessageReasonNoPendingInput means there is no matching input request.
	SessionMessageReasonNoPendingInput = "NoPendingInput"
)

// SessionMessageSpec defines a request delivered to a running Session.
//
// +kubebuilder:validation:XValidation:rule="self.type != 'message' || (has(self.text) && size(self.text) > 0)",message="text is required when type is message"
// +kubebuilder:validation:XValidation:rule="self.type != 'input' || has(self.inputID) || has(self.taskRef)",message="inputID or taskRef is required when type is input"
// +kubebuilder:validation:XValidation:rule="self.type != 'input' || (has(self.cancel) && self.cancel) || (has(self.answers) && size(self.answers) > 0)",message="answers or cancel is required when type is input"
// +kubebuilder:validation:XValidation:rule="self.type != 'remove' || has(self.taskRef)",message="taskRef is required when type is remove"
type SessionMessageSpec struct {
	// SessionName is the name of the Session in the same namespace that receives
	// this message.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	SessionName string `json:"sessionName"`

	// Type selects what the Session runtime does with this message.
	// +kubebuilder:validation:Enum=message;input;interrupt;remove
	Type SessionMessageType `json:"type"`

	// Text is the user message submitted when type is message. It is submitted
	// as plain text; terminal and web chat commands such as !COMMAND and /goal are
	// not interpreted.
	// +optional
	// +kubebuilder:validation:MaxLength=65536
	Text string `json:"text,omitempty"`

	// InputID identifies the pending input request answered when type is input.
	// When omitted, the input request reported in the status of the SessionMessage
	// named by TaskRef is answered.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	InputID string `json:"inputID,omitempty"`

	// Answers maps input question IDs to their answers when type is input.
	// +optional
	// +kubebuilder:validation:MaxProperties=32
	Answers map[string]SessionInputAnswer `json:"answers,omitempty"`

	// Cancel cancels the pending input request instead of answering it when type
	// is input.
	// +optional
	Cancel bool `json:"cancel,omitempty"`

	// TaskRef names an earlier SessionMessage of type message for the same Session.
	// Type input answers that message's pending input request when InputID is
	// omitted. Type remove removes the pending turn that carries that message.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	TaskRef string `json:"taskRef,omitempty"`
}

// SessionInputAnswer lists the answers given to one input question.
// +kubebuilder:validation:MaxItems=32
// +kubebuilder:validation:items:MaxLength=4096
type SessionInputAnswer []string

// SessionInputOption describes one structured answer offered for an input question.
type SessionInputOption struct {
	// Label is the answer value.
	Label string `json:"label"`

	// Description explains the answer.
	// +optional
	Description string `json:"description,omitempty"`
}

// SessionInputQuestion describes one question that blocks the active turn.
type SessionInputQuestion struct {
	// ID identifies the question in SessionMessage.spec.answers.
	ID string `json:"id"`

	// Header is a short label for the question.
	// +optional
	Header string `json:"header,omitempty"`

	// Question is the question text.
	Question string `json:"question"`

	// Options lists the structured answers offered for the question.
	// +optional
	Options []SessionInputOption `json:"options,omitempty"`

	// MultiSelect reports whether more than one option may be selected.
	// +optional
	MultiSelect bool `json:"multiSelect,omitempty"`

	// Secret reports whether the answer should be treated as sensitive.
	// +optional
	Secret bool `json:"secret,omitempty"`
}

// SessionMessageInput describes an input request raised by the turn carrying a message.
type SessionMessageInput struct {
	// ID identifies the input request in SessionMessage.spec.inputID.
	ID string `json:"id"`

	// Questions lists the questions that must be answered.
	// +optional
	Questions []SessionInputQuestion `json:"questions,omitempty"`
}

// SessionMessageResult holds the final assistant response of a completed turn.
type SessionMessageResult struct {
	// Text is the last assistant message of the turn, truncated to 16384 characters.
	// +optional
	// +kubebuilder:validation:MaxLength=16384
	Text string `json:"text,omitempty"`

	// Truncated reports whether Text was shortened.
	// +optional
	Truncated bool `json:"truncated,omitempty"`
}

// SessionMessageStatus defines the observed state of a SessionMessage.
type SessionMessageStatus struct {
	// Phase is the delivery and outcome phase of the message. An empty phase
	// means Pending.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Accepted;Working;InputRequired;Completed;Failed;Canceled;Rejected
	Phase SessionMessagePhase `json:"phase,omitempty"`

	// PodUID identifies the Session Pod that processed the message.
	// +optional
	PodUID types.UID `json:"podUID,omitempty"`

	// RequestID is the runtime request ID used to deliver the message.
	// +optional
	RequestID string `json:"requestID,omitempty"`

	// TurnID identifies the Session turn that carries the message.
	// +optional
	TurnID string `json:"turnID,omitempty"`

	// MergedInto names the SessionMessage whose pending turn this message was
	// appended to, when the message did not start a turn of its own.
	// +optional
	MergedInto string `json:"mergedInto,omitempty"`

	// Input describes the pending input request while Phase is InputRequired.
	// +optional
	Input *SessionMessageInput `json:"input,omitempty"`

	// Result holds the final assistant response when Phase is Completed.
	// +optional
	Result *SessionMessageResult `json:"result,omitempty"`

	// Reason is a machine-readable explanation for the current phase.
	// +optional
	Reason string `json:"reason,omitempty"`

	// Message is a human-readable explanation for the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// AcceptedAt is when the runtime accepted the message.
	// +optional
	AcceptedAt *metav1.Time `json:"acceptedAt,omitempty"`

	// CompletedAt is when the message reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// +genclient
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=smsg
// +kubebuilder:printcolumn:name="Session",type=string,JSONPath=`.spec.sessionName`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SessionMessage delivers a message, input answer, interrupt, or pending-turn
// removal to a running Session.
type SessionMessage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec is immutable"
	Spec   SessionMessageSpec   `json:"spec"`
	Status SessionMessageStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SessionMessageList contains a list of SessionMessage.
type SessionMessageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SessionMessage `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SessionMessage{}, &SessionMessageList{})
}
