package eu.stratosphere.streaming.examples.basictopology;

import java.net.InetSocketAddress;

import org.apache.log4j.Level;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.util.LogUtils;

public class BasicTopology {

	public static class BasicSource extends UserSourceInvokable {

		StreamRecord record = new StreamRecord(new Tuple1<String>("streaming"));

		@Override
		public void invoke() throws Exception {

			while (true) {
				// continuously emit records
				emit(record);
				performanceCounter.count();
			}

		}
	}

	public static class BasicTask extends UserTaskInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			// send record to sink without any modifications
			emit(record);
			performanceCounter.count();
		}

	}

	public static class BasicSink extends UserSinkInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			// do nothing
		}
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("BasicStreamingTopology");
		graphBuilder.setSource("BasicSource", BasicSource.class, 1, 1);
		graphBuilder.setTask("BasicTask", BasicTask.class, 1, 1);
		graphBuilder.setSink("BasicSink", BasicSink.class, 1, 1);

		graphBuilder.shuffleConnect("BasicSource", "BasicTask");
		graphBuilder.shuffleConnect("BasicTask", "BasicSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		// set logging parameters for local run
		LogUtils.initializeDefaultConsoleLogger(Level.ERROR, Level.INFO);

		try {

			// generate JobGraph
			JobGraph jG = getJobGraph();
			Configuration configuration = jG.getJobConfiguration();

			if (args.length == 0 || args[0].equals("local")) {
				System.out.println("Running in Local mode");
				// start local cluster and submit JobGraph
				NepheleMiniCluster exec = new NepheleMiniCluster();
				exec.start();

				Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);

				client.run(jG, true);

				exec.stop();
			} else if (args[0].equals("cluster")) {
				System.out.println("Running in Cluster mode");
				// submit JobGraph to the running cluster
				Client client = new Client(new InetSocketAddress("dell150", 6123), configuration);
				client.run(jG, true);
			}

		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
