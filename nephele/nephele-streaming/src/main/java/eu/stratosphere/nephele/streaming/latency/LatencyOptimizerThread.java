package eu.stratosphere.nephele.streaming.latency;

import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.streaming.AbstractStreamingData;
import eu.stratosphere.nephele.streaming.PathLatency;

public class LatencyOptimizerThread extends Thread {
	
	private LinkedBlockingQueue<AbstractStreamingData> streamingDataQueue;
	
	private ExecutionGraph executionGraph;
	
	private LatencyModel latencyModel;
	
	public LatencyOptimizerThread(ExecutionGraph executionGraph) {
		this.latencyModel = new LatencyModel(executionGraph);
		this.streamingDataQueue = new LinkedBlockingQueue<AbstractStreamingData>();
	}
	
	public void run() {

		try {
			while (!interrupted()) {
				AbstractStreamingData streamingData = streamingDataQueue.take();
				
				if (streamingData instanceof PathLatency) {
					latencyModel.refreshEdgeLatency((PathLatency) streamingData);
				}

			}

		} catch (InterruptedException e) {
		}
	}
	
	public void handOffStreamingData(AbstractStreamingData data) {
		streamingDataQueue.add(data);
	}
}
