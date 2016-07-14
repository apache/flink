package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import static java.util.Objects.requireNonNull;

/**
 * Message sent by the callback handler to the scheduler actor
 * when the scheduler successfully registers with a Mesos master.
 */
public class Registered extends Connected {

	private org.apache.mesos.Protos.FrameworkID frameworkId;
	private Protos.MasterInfo masterInfo;

	public Registered(Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
		requireNonNull(frameworkId);
		requireNonNull(masterInfo);

		this.frameworkId = frameworkId;
		this.masterInfo = masterInfo;
	}

	public Protos.FrameworkID frameworkId() {
		return frameworkId;
	}

	public Protos.MasterInfo masterInfo() {
		return masterInfo;
	}

	@Override
	public String toString() {
		return "Registered{" +
			"frameworkId=" + frameworkId +
			", masterInfo=" + masterInfo +
			'}';
	}
}
