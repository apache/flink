package eu.stratosphere.nephele.streaming.latency;

import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.streaming.StreamingJobManagerPlugin;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class LatencyOptimizerThread extends Thread {

	private Log LOG = LogFactory.getLog(LatencyOptimizerThread.class);

	private final LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private final StreamingJobManagerPlugin jobManagerPlugin;

	private final ExecutionGraph executionGraph;

	private final LatencyModel latencyModel;

	public LatencyOptimizerThread(StreamingJobManagerPlugin jobManagerPlugin, ExecutionGraph executionGraph) {
		this.jobManagerPlugin = jobManagerPlugin;
		this.executionGraph = executionGraph;
		this.latencyModel = new LatencyModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
	}

	public void run() {
		LOG.info("Started optimizer thread for job " + executionGraph.getJobName());

		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();

				if (streamingData instanceof ChannelLatency) {
					latencyModel.refreshEdgeLatency((ChannelLatency) streamingData);
				} else if (streamingData instanceof TaskLatency) {
					latencyModel.refreshTaskLatency((TaskLatency) streamingData);
				} else if (streamingData instanceof ChannelThroughput) {
					latencyModel.refreshChannelThroughput((ChannelThroughput) streamingData);
				}
			}

		} catch (InterruptedException e) {
		}

		LOG.info("Stopped optimizer thread for job " + executionGraph.getJobName());
	}

	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}

	public void limitBufferSize(ManagementEdgeID sourceEdgeID, int bufferSize) {

		final ChannelID sourceChannelID = sourceEdgeID.toChannelID();
		final ExecutionVertex vertex = this.executionGraph.getVertexByChannelID(sourceChannelID);
		if (vertex == null) {
			LOG.error("Cannot find vertex to channel ID " + vertex);
			return;
		}

		this.jobManagerPlugin.limitBufferSize(vertex, sourceChannelID, bufferSize);
	}
}
