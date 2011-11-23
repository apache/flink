package eu.stratosphere.nephele.streaming.latency;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class LatencyModel {

	// private static Log LOG = LogFactory.getLog(LatencyModel.class);

	private ExecutionGraph executionGraph;

	private LatencySubgraph latencySubgraph;

	public LatencyModel(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;

		// FIXME naive implementation until we can annotate the job
		// subgraphStart and subgraphEnd should be derived from the annotations
		ExecutionGroupVertex subgraphStart = this.executionGraph.getInputVertex(0).getGroupVertex();
		ExecutionGroupVertex subgraphEnd = this.executionGraph.getOutputVertex(0).getGroupVertex();

		this.latencySubgraph = new LatencySubgraph(executionGraph, subgraphStart, subgraphEnd);
	}

	public void refreshEdgeLatency(ChannelLatency channelLatency) {

		ManagementEdgeID sourceEdgeID = latencySubgraph.getEdgeByReceiverVertexID(channelLatency.getSinkVertexID()
			.toManagementVertexID());

		EdgeCharacteristics edgeLatency = latencySubgraph.getEdgeCharacteristicsBySourceEdgeID(sourceEdgeID);
		edgeLatency.addLatencyMeasurement(System.currentTimeMillis(), channelLatency.getChannelLatency());
	}

	public void refreshTaskLatency(TaskLatency taskLatency) {
		VertexLatency vertexLatency = latencySubgraph
			.getVertexLatency(taskLatency.getVertexID().toManagementVertexID());
		vertexLatency.addLatencyMeasurement(System.currentTimeMillis(), taskLatency.getTaskLatency());
		i++;

		if (i % 20 == 0) {
			for (LatencyPath path : latencySubgraph.getLatencyPaths()) {
				path.dumpLatencies();
			}
		}
	}

	// FIXME this should be removed later on
	public int i = 0;

	public void refreshChannelThroughput(ChannelThroughput channelThroughput) {
		ManagementEdgeID edgeID = new ManagementEdgeID(channelThroughput.getSourceChannelID());
		EdgeCharacteristics edgeCharaceristics = latencySubgraph.getEdgeCharacteristicsBySourceEdgeID(edgeID);
		edgeCharaceristics.addThroughputMeasurement(System.currentTimeMillis(), channelThroughput.getThroughput());

		// FIXME this should be removed later on
		i++;
		if (i % 20 == 0) {
			for (LatencyPath path : latencySubgraph.getLatencyPaths()) {
				path.dumpLatencies();
			}
		}
	}
}
