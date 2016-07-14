package org.apache.flink.mesos.scheduler;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;

/**
 * A builder for the Fenzo task scheduler.
 *
 * Note that the Fenzo-provided {@link TaskScheduler.Builder} cannot be mocked, which motivates this interface.
 */
public interface TaskSchedulerBuilder {
	TaskSchedulerBuilder withLeaseRejectAction(Action1<VirtualMachineLease> action);

	TaskScheduler build();
}
