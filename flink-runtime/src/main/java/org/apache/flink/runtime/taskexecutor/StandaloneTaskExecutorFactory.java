package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.rpc.RpcService;

/**
 * An factory for creating {@link TaskExecutor} and starting it in standalone mode.
 */
public class StandaloneTaskExecutorFactory extends TaskExecutorFactory {

	/** RPC service to be used to start the RPC server and to obtain RPC gateways for the task manager */
	private final RpcService rpcService;

	/** The access to the leader election and retrieval services for the task manager */
	private final HighAvailabilityServices haServices;

	/** The hostname/address that describes the TaskManager's data location */
	private final String taskManagerHostname;

	public StandaloneTaskExecutorFactory(
			Configuration configuration,
			ResourceID resourceID,
			RpcService rpcService,
			String taskManagerHostname,
			HighAvailabilityServices haServices) {

		super(configuration, resourceID);

		this.rpcService = rpcService;
		this.taskManagerHostname = taskManagerHostname;
		this.haServices = haServices;
	}

	@Override
	public TaskExecutor createAndStartTaskExecutor() throws Exception{
		TaskExecutor taskExecutor = startTaskManagerComponentsAndActor(
			configuration,
			resourceID,
			rpcService,
			taskManagerHostname,
			haServices,
			true);

		taskExecutor.start();

		return taskExecutor;
	}
}
