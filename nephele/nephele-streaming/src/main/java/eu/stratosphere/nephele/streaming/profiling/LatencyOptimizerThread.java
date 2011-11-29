package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.StreamingJobManagerPlugin;
import eu.stratosphere.nephele.streaming.buffers.BufferSizeManager;
import eu.stratosphere.nephele.streaming.types.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class LatencyOptimizerThread extends Thread {

	private Log LOG = LogFactory.getLog(LatencyOptimizerThread.class);

	private final LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;

	private final StreamingJobManagerPlugin jobManagerPlugin;

	private final ExecutionGraph executionGraph;

	private final ProfilingModel profilingModel;

	private ProfilingLogger logger;

	private BufferSizeManager bufferSizeManager;

	public LatencyOptimizerThread(StreamingJobManagerPlugin jobManagerPlugin, ExecutionGraph executionGraph) {
		this.jobManagerPlugin = jobManagerPlugin;
		this.executionGraph = executionGraph;
		this.profilingModel = new ProfilingModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
		try {
			this.logger = new ProfilingLogger();
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}

		this.bufferSizeManager = new BufferSizeManager(200, this.profilingModel, this.jobManagerPlugin,
			this.executionGraph);
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
				} else if (streamingData instanceof OutputBufferLatency) {
					profilingModel.refreshChannelOutputBufferLatency(now, (OutputBufferLatency) streamingData);
				}

				if (this.logger.isLoggingNecessary(now)) {
					ProfilingSummary summary = profilingModel.computeProfilingSummary();
					try {
						logger.logLatencies(summary);
					} catch (IOException e) {
						LOG.error("Error when writing to profiling logger file", e);
					}

					if (bufferSizeManager.isAdjustmentNecessary(now)) {
						bufferSizeManager.adjustBufferSizes(summary);
					}
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
