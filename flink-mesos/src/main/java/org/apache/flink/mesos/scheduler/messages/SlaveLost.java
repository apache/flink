package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

/**
 * Message sent by the callback handler to the scheduler actor
 * when a slave has been determined unreachable (e.g., machine failure, network partition).
 */
public class SlaveLost {
	private Protos.SlaveID slaveId;

	public SlaveLost(Protos.SlaveID slaveId) {
		this.slaveId = slaveId;
	}

	public Protos.SlaveID slaveId() {
		return slaveId;
	}

	@Override
	public String toString() {
		return "SlaveLost{" +
			"slaveId=" + slaveId +
			'}';
	}
}
