package eu.stratosphere.nephele.streaming.profiling;

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

	private final ProfilingModel profilingModel;

	public LatencyOptimizerThread(StreamingJobManagerPlugin jobManagerPlugin, ExecutionGraph executionGraph) {
		this.jobManagerPlugin = jobManagerPlugin;
		this.executionGraph = executionGraph;
		this.profilingModel = new ProfilingModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
	}

	public void run() {
		LOG.info("Started optimizer thread for job " + executionGraph.getJobName());

		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();

				long now = System.currentTimeMillis();
				if (streamingData instanceof ChannelLatency) {
					profilingModel.refreshEdgeLatency(now, (ChannelLatency) streamingData);
				} else if (streamingData instanceof TaskLatency) {
					profilingModel.refreshTaskLatency(now, (TaskLatency) streamingData);
				} else if (streamingData instanceof ChannelThroughput) {
					profilingModel.refreshChannelThroughput(now, (ChannelThroughput) streamingData);
				}

				profilingModel.logProfilingSummaryIfNecessary(now);
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
