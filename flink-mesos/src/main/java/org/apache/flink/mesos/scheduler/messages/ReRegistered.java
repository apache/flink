package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import static java.util.Objects.requireNonNull;

/**
 * Message sent by the callback handler to the scheduler actor
 * when the scheduler re-registers with a newly elected Mesos master.
 */
public class ReRegistered extends Connected {
	private Protos.MasterInfo masterInfo;

	public ReRegistered(Protos.MasterInfo masterInfo) {
		requireNonNull(masterInfo);

		this.masterInfo = masterInfo;
	}

	public Protos.MasterInfo masterInfo() {
		return masterInfo;
	}

	@Override
	public String toString() {
		return "ReRegistered{" +
			"masterInfo=" + masterInfo +
			'}';
	}
}
