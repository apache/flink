package eu.stratosphere.nephele.instance.cloud;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * This class is an auxiliary class to send the notification
 * about the availability of an {@link AbstractInstance} to the given {@link InstanceListener} object. The notification
 * must be sent from
 * a separate thread, otherwise the atomic operation of requesting an instance
 * for a vertex and switching to the state ASSINING could not be guaranteed.
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class CloudInstanceNotifier extends Thread {

	/**
	 * The {@link InstanceListener} object to send the notification to.
	 */
	private final InstanceListener instanceListener;

	private final CloudInstance instance;

	private final JobID id;

	/**
	 * Constructs a new instance notifier object.
	 * 
	 * @param instanceListener
	 *        the listener to send the notification to
	 * @param allocatedSlice
	 *        the slice with has been allocated for the job
	 */
	public CloudInstanceNotifier(InstanceListener instanceListener, JobID id, CloudInstance instance) {
		this.instanceListener = instanceListener;
		this.instance = instance;
		this.id = id;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void run() {
		this.instanceListener.resourceAllocated(id, instance.asAllocatedResource());
	}
}
