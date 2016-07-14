package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;

import static java.util.Objects.requireNonNull;

public class MesosTaskManagerParameters {

	private double cpus;

	private ContaineredTaskManagerParameters containeredParameters;

	public MesosTaskManagerParameters(double cpus, ContaineredTaskManagerParameters containeredParameters) {
		requireNonNull(containeredParameters);
		this.cpus = cpus;
		this.containeredParameters = containeredParameters;
	}

	public double cpus() {
		return cpus;
	}

	public ContaineredTaskManagerParameters containeredParameters() {
		return containeredParameters;
	}

	@Override
	public String toString() {
		return "MesosTaskManagerParameters{" +
			"cpus=" + cpus +
			", containeredParameters=" + containeredParameters +
			'}';
	}

	/**
	 * Create the Mesos TaskManager parameters.
	 * @param flinkConfig the TM configuration.
	 * @param containeredParameters additional containered parameters.
     */
	public static MesosTaskManagerParameters create(
		Configuration flinkConfig,
		ContaineredTaskManagerParameters containeredParameters) {

		double cpus = flinkConfig.getDouble(ConfigConstants.MESOS_RESOURCEMANAGER_TASKS_CPUS,
			Math.max(containeredParameters.numSlots(), 1.0));

		return new MesosTaskManagerParameters(cpus, containeredParameters);
	}
}
