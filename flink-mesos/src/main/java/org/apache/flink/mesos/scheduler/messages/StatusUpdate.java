package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

/**
 * Message sent by the callback handler to the scheduler actor
 * when the status of a task has changed (e.g., a slave is lost and so the task is lost,
 * a task finishes and an executor sends a status update saying so, etc).
 */
public class StatusUpdate {
	private Protos.TaskStatus status;

	public StatusUpdate(Protos.TaskStatus status) {
		this.status = status;
	}

	public Protos.TaskStatus status() {
		return status;
	}

	@Override
	public String toString() {
		return "StatusUpdate{" +
			"status=" + status +
			'}';
	}
}
