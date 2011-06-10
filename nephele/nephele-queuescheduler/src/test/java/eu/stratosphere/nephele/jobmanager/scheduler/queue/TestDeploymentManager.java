/**
 * This abstract scheduler must be extended by a scheduler implementations for Nephele. The abstract class defines the
 * fundamental methods for scheduling and removing jobs. While Nephele's
 * {@link eu.stratosphere.nephele.jobmanager.JobManager} is responsible for requesting the required instances for the
 * job at the {@link eu.stratosphere.nephele.instance.InstanceManager}, the scheduler is in charge of assigning the
 * individual tasks to the instances.
 * 
 * @author warneke
 */

package eu.stratosphere.nephele.jobmanager.scheduler.queue;

import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.DeploymentManager;

/**
 * This class provides an implementation of the {@DeploymentManager} interface which is used during
 * the unit tests.
 * 
 * @author warneke
 */
public class TestDeploymentManager implements DeploymentManager {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void deploy(final JobID jobID, final AbstractInstance instance,
			final List<ExecutionVertex> verticesToBeDeployed) {
		// TODO Auto-generated method stub

	}

}
