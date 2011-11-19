package eu.stratosphere.nephele.streaming.latency;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.PathLatency;

public class LatencyOptimizerThread extends Thread {

	private Log LOG = LogFactory.getLog(LatencyOptimizerThread.class);

	private LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private ExecutionGraph executionGraph;

	private LatencyModel latencyModel;

	public LatencyOptimizerThread(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;
		this.latencyModel = new LatencyModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
	}

	public void run() {
		LOG.info("Started optimizer thread for job " + executionGraph.getJobName());

		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();

				if (streamingData instanceof PathLatency) {
					latencyModel.refreshEdgeLatency((PathLatency) streamingData);
				}

			}

		} catch (InterruptedException e) {
		}

		LOG.info("Stopped optimizer thread for job " + executionGraph.getJobName());
	}

	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}
}
