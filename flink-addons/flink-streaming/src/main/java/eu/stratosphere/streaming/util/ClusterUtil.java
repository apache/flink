package eu.stratosphere.streaming.util;

import java.net.InetSocketAddress;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.ProgramInvocationException;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;

public class ClusterUtil {

	/**
	 * Executes the given JobGraph locally, on a NepheleMiniCluster
	 * 
	 * @param jobGraph
	 */
	public static void runOnMiniCluster(JobGraph jobGraph) {
		System.out.println("Running on mini cluster");

		Configuration configuration = jobGraph.getJobConfiguration();

		NepheleMiniCluster exec = new NepheleMiniCluster();
		Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);

		try {
			exec.start();

			client.run(jobGraph, true);

			exec.stop();
		} catch (Exception e) {
		}
	}

	public static void runOnLocalCluster(JobGraph jobGraph, String IP, int port) {
		System.out.println("Running on local cluster");
		Configuration configuration = jobGraph.getJobConfiguration();

		Client client = new Client(new InetSocketAddress(IP, port), configuration);

		try {
			client.run(jobGraph, true);
		} catch (ProgramInvocationException e) {
		}
	}

}
