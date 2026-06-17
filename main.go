package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ActivityState represents the state of an activity.
type ActivityState string

const (
	StateScheduled ActivityState = "Scheduled"
	StateStarted   ActivityState = "Started"
	StateTimedOut  ActivityState = "TimedOut"
	StateCompleted ActivityState = "Completed"
)

// ActivityTask represents a task in the matching queue.
type ActivityTask struct {
	ID         string
	ActivityID string
	ScheduleID int64
	Attempt    int32
	Invalid    bool
}

// HistoryEngine manages the mutable state of workflows and activities.
type HistoryEngine struct {
	mu             sync.Mutex
	activities     map[string]*ActivityInfo
	matchingEngine *MatchingEngine
}

type ActivityInfo struct {
	ID         string
	State      ActivityState
	ScheduleID int64
	Attempt    int32
}

// MatchingEngine manages task queues and dispatches tasks to pollers.
type MatchingEngine struct {
	mu            sync.Mutex
	queue         map[string][]*ActivityTask
	tombstones    map[string]bool
	historyEngine *HistoryEngine
}

func NewHistoryEngine() *HistoryEngine {
	return &HistoryEngine{
		activities: make(map[string]*ActivityInfo),
	}
}

func NewMatchingEngine(history *HistoryEngine) *MatchingEngine {
	return &MatchingEngine{
		queue:         make(map[string][]*ActivityTask),
		tombstones:    make(map[string]bool),
		historyEngine: history,
	}
}

// ScheduleActivity schedules a new activity task.
func (h *HistoryEngine) ScheduleActivity(activityID string, scheduleID int64, attempt int32, taskQueue string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.activities[activityID] = &ActivityInfo{
		ID:         activityID,
		State:      StateScheduled,
		ScheduleID: scheduleID,
		Attempt:    attempt,
	}

	task := &ActivityTask{
		ID:         fmt.Sprintf("%s-%d-%d", activityID, scheduleID, attempt),
		ActivityID: activityID,
		ScheduleID: scheduleID,
		Attempt:    attempt,
	}
	h.matchingEngine.AddTask(taskQueue, task)
	fmt.Printf("[History] Scheduled activity %s (Attempt %d)\n", activityID, attempt)
}

// ProcessHeartbeatTimeout handles the heartbeat timeout event.
func (h *HistoryEngine) ProcessHeartbeatTimeout(activityID string) {
	h.mu.Lock()
	act, exists := h.activities[activityID]
	if !exists || act.State != StateStarted {
		h.mu.Unlock()
		return
	}

	fmt.Printf("[History] Heartbeat timeout triggered for activity %s\n", activityID)
	act.State = StateTimedOut
	taskID := fmt.Sprintf("%s-%d-%d", activityID, act.ScheduleID, act.Attempt)
	h.mu.Unlock()

	// 1. Tombstoning/Validation: Invalidate the task in Matching service
	h.matchingEngine.TombstoneTask(taskID)
}

// RecordActivityTaskStarted verifies and transitions the activity to Started state.
func (h *HistoryEngine) RecordActivityTaskStarted(activityID string, scheduleID int64, attempt int32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	act, exists := h.activities[activityID]
	if !exists {
		return errors.New("activity not found")
	}

	// 2. Token Verification: Strictly guard against duplicate dispatches of timed-out/rescheduled activities
	if act.State != StateScheduled {
		return fmt.Errorf("cannot start activity: current state is %s (expected Scheduled)", act.State)
	}
	if act.ScheduleID != scheduleID || act.Attempt != attempt {
		return fmt.Errorf("cannot start activity: stale scheduleID/attempt")
	}

	act.State = StateStarted
	fmt.Printf("[History] Activity %s started successfully (Attempt %d)\n", activityID, attempt)
	return nil
}

func (m *MatchingEngine) AddTask(taskQueue string, task *ActivityTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue[taskQueue] = append(m.queue[taskQueue], task)
}

func (m *MatchingEngine) TombstoneTask(taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tombstones[taskID] = true
	fmt.Printf("[Matching] Tombstoned task %s\n", taskID)
}

// PollAndDispatch simulates a worker polling and matching service dispatching a task.
func (m *MatchingEngine) PollAndDispatch(ctx context.Context, taskQueue string, workerID string) (*ActivityTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tasks, exists := m.queue[taskQueue]
	if !exists || len(tasks) == 0 {
		return nil, errors.New("no tasks in queue")
	}

	// Pop task
	task := tasks[0]
	m.queue[taskQueue] = tasks[1:]

	// 3. Rebalance Guard: Verify task validity before dispatching
	if m.tombstones[task.ID] {
		fmt.Printf("[Matching] Dropped tombstoned task %s during dispatch to %s\n", task.ID, workerID)
		return nil, errors.New("task is tombstoned")
	}

	// Double check with History mutable state to ensure it hasn't timed out/rescheduled
	m.historyEngine.mu.Lock()
	act, exists := m.historyEngine.activities[task.ActivityID]
	if !exists || act.State != StateScheduled || act.ScheduleID != task.ScheduleID || act.Attempt != task.Attempt {
		m.historyEngine.mu.Unlock()
		fmt.Printf("[Matching] Dropped stale task %s during dispatch to %s (History state mismatch)\n", task.ID, workerID)
		return nil, errors.New("task is stale according to history state")
	}
	m.historyEngine.mu.Unlock()

	fmt.Printf("[Matching] Dispatched task %s to worker %s\n", task.ID, workerID)
	return task, nil
}

func main() {
	fmt.Println("Starting Temporal Activity Heartbeat Timeout Race Condition Simulation...")

	history := NewHistoryEngine()
	matching := NewMatchingEngine(history)
	history.matchingEngine = matching

	taskQueue := "test-task-queue"
	activityID := "activity-1"

	// Scenario: Normal execution flow
	fmt.Println("\n--- Scenario 1: Normal Execution ---")
	history.ScheduleActivity(activityID, 1, 1, taskQueue)

	// Worker 1 polls and starts the activity
	task, err := matching.PollAndDispatch(context.Background(), taskQueue, "worker-1")
	if err == nil {
		err = history.RecordActivityTaskStarted(task.ActivityID, task.ScheduleID, task.Attempt)
		if err != nil {
			fmt.Printf("Worker 1 failed to start activity: %v\n", err)
		}
	}

	// Scenario 2: Heartbeat timeout fires during a simulated rebalance/duplicate dispatch
	fmt.Println("\n--- Scenario 2: Heartbeat Timeout during Rebalance ---")
	// Trigger heartbeat timeout
	history.ProcessHeartbeatTimeout(activityID)

	// Simulate a stale/duplicate task being dispatched to Worker 2 due to rebalance/reload
	// We manually re-queue the task to simulate a duplicate queue entry or partition reload
	staleTask := &ActivityTask{
		ID:         fmt.Sprintf("%s-%d-%d", activityID, 1, 1),
		ActivityID: activityID,
		ScheduleID: 1,
		Attempt:    1,
	}
	matching.AddTask(taskQueue, staleTask)

	// Worker 2 attempts to poll and execute the stale task
	fmt.Println("[Worker 2] Polling for task...")
	task2, err := matching.PollAndDispatch(context.Background(), taskQueue, "worker-2")
	if err != nil {
		fmt.Printf("[Worker 2] Poll failed: %v\n", err)
	} else {
		err = history.RecordActivityTaskStarted(task2.ActivityID, task2.ScheduleID, task2.Attempt)
		if err != nil {
			fmt.Printf("[Worker 2] Start failed: %v\n", err)
		}
	}

	fmt.Println("\nSimulation finished successfully. Duplicate execution prevented.")
}