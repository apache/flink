package eu.stratosphere.api.datastream;

import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;

public class StreamExecutionEnvironment {
	JobGraphBuilder jobGraphBuilder;
	
	public StreamExecutionEnvironment() {
		jobGraphBuilder = new JobGraphBuilder("jobGraph",FaultToleranceType.NONE);
	}
	
//	public static StreamExecutionEnvironment getLocalEnvironment() {
//		return new StreamExecutionEnvironment();
//	}
	
	public void execute() {
		ClusterUtil.runOnMiniCluster(jobGraphBuilder.getJobGraph());
	}
}
