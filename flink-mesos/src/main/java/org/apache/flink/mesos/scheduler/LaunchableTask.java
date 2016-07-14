package org.apache.flink.mesos.scheduler;

import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import org.apache.mesos.Protos;

/**
 * Specifies the task requirements and produces a Mesos TaskInfo description.
 */
public interface LaunchableTask {

	/**
	 * Get a representation of the task requirements as understood by Fenzo.
     */
	TaskRequest taskRequest();

	/**
	 * Prepare to launch the task by producing a Mesos TaskInfo record.
	 * @param slaveId the slave assigned to the task.
	 * @param taskAssignmentResult the task assignment details.
     * @return a TaskInfo.
     */
	Protos.TaskInfo launch(Protos.SlaveID slaveId, TaskAssignmentResult taskAssignmentResult);
}
