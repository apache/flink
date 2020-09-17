package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class RestartIndividualFailoverStrategy implements FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RestartIndividualFailoverStrategy.class);

	RestartIndividualFailoverStrategy(
			SchedulingTopology topology,
			ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {
	}

	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause) {
		return ImmutableSet.of(executionVertexId);
	}

	/**
	 * The factory to instantiate {@link RestartIndividualFailoverStrategy}.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(
			final SchedulingTopology topology,
			final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

			return new RestartIndividualFailoverStrategy(topology, resultPartitionAvailabilityChecker);
		}
	}
}
