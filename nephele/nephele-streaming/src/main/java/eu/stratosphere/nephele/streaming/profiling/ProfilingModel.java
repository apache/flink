package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.OutputBufferLatency;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class ProfilingModel {

//	private static Log LOG = LogFactory.getLog(ProfilingModel.class);

	private ExecutionGraph executionGraph;

	private ProfilingSubgraph profilingSubgraph;

	public ProfilingModel(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;

		// FIXME naive implementation until we can annotate the job
		// subgraphStart and subgraphEnd should be derived from the annotations
		ExecutionGroupVertex subgraphStart = this.executionGraph.getInputVertex(0).getGroupVertex();
		ExecutionGroupVertex subgraphEnd = this.executionGraph.getOutputVertex(0).getGroupVertex();

		this.profilingSubgraph = new ProfilingSubgraph(executionGraph, subgraphStart, subgraphEnd, false, false);
	}

	public void refreshEdgeLatency(long timestamp, ChannelLatency channelLatency) {
		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(channelLatency.getChannelLatency()) || Double.isNaN(channelLatency.getChannelLatency())) {
			return;
		}

		// FIXME: workaround for bug caused by streaming plugin
		if (!channelLatency.getSourceVertexID().equals(channelLatency.getSinkVertexID())) {

			XoredVertexID xored = new XoredVertexID(channelLatency.getSourceVertexID().toManagementVertexID(),
				channelLatency.getSinkVertexID().toManagementVertexID());

			ManagementEdgeID sourceEdgeID = profilingSubgraph.getSourceEdgeIDByXoredVertexID(xored);

			if (sourceEdgeID == null) {
				ExecutionVertex source = executionGraph.getVertexByID(channelLatency.getSourceVertexID());
				ExecutionVertex sink = executionGraph.getVertexByID(channelLatency.getSinkVertexID());

				throw new RuntimeException("No source edge ID for " + ProfilingUtils.formatName(source) + "->" + ProfilingUtils.formatName(sink) + " "
					+ xored.toString());
			}

			EdgeCharacteristics edgeCharacteristics = profilingSubgraph
				.getEdgeCharacteristicsBySourceEdgeID(sourceEdgeID);

			edgeCharacteristics.addLatencyMeasurement(timestamp, channelLatency.getChannelLatency());
		}
	}

	public void refreshTaskLatency(long timestamp, TaskLatency taskLatency) {
		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(taskLatency.getTaskLatency()) || Double.isNaN(taskLatency.getTaskLatency())) {
			return;
		}
		
		VertexLatency vertexLatency = profilingSubgraph
			.getVertexLatency(taskLatency.getVertexID().toManagementVertexID());
		vertexLatency.addLatencyMeasurement(timestamp, taskLatency.getTaskLatency());
	}

	public void refreshChannelThroughput(long timestamp, ChannelThroughput channelThroughput) {

		// FIXME workaround for bug that causes NaNs
		if (Double.isInfinite(channelThroughput.getThroughput()) || Double.isNaN(channelThroughput.getThroughput())) {
			return;
		}

		ManagementEdgeID edgeID = new ManagementEdgeID(channelThroughput.getSourceChannelID());
		EdgeCharacteristics edgeCharaceristics = profilingSubgraph.getEdgeCharacteristicsBySourceEdgeID(edgeID);
		edgeCharaceristics.addThroughputMeasurement(timestamp, channelThroughput.getThroughput());
	}
	
	public void refreshChannelOutputBufferLatency(long timestamp, OutputBufferLatency latency) {
		ManagementEdgeID sourceEdgeID = new ManagementEdgeID(latency.getSourceChannelID());
		EdgeCharacteristics edgeCharaceristics = profilingSubgraph.getEdgeCharacteristicsBySourceEdgeID(sourceEdgeID);
		edgeCharaceristics.addOutputBufferLatencyMeasurement(timestamp, latency.getBufferLatency());
	}

	public ProfilingSummary computeProfilingSummary() {
		return new ProfilingSummary(profilingSubgraph);
	}

	public ProfilingSubgraph getProfilingSubgraph() {
		return profilingSubgraph;
	}
}
