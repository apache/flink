package eu.stratosphere.nephele.streaming.profiling;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.streaming.types.ChannelLatency;
import eu.stratosphere.nephele.streaming.types.ChannelThroughput;
import eu.stratosphere.nephele.streaming.types.TaskLatency;

public class ProfilingModel {

	private static Log LOG = LogFactory.getLog(ProfilingModel.class);

	private final static long WAIT_INTERVAL_BEFORE_LOGGING = 10 * 1000;

	private final static long LOGGING_INTERVAL = 1000;

	private ExecutionGraph executionGraph;

	private ProfilingSubgraph profilingSubgraph;

	private ProfilingLogger logger;

	private long timeOfLastLogging;

	private long timeBase;

	public ProfilingModel(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;

		// FIXME naive implementation until we can annotate the job
		// subgraphStart and subgraphEnd should be derived from the annotations
		ExecutionGroupVertex subgraphStart = this.executionGraph.getInputVertex(0).getGroupVertex();
		ExecutionGroupVertex subgraphEnd = this.executionGraph.getOutputVertex(0).getGroupVertex();

		this.profilingSubgraph = new ProfilingSubgraph(executionGraph, subgraphStart, subgraphEnd, false, false);

		try {
			this.logger = new ProfilingLogger(profilingSubgraph);
		} catch (IOException e) {
			LOG.error("Error when opening profiling logger file", e);
		}
		this.timeOfLastLogging = System.currentTimeMillis() + WAIT_INTERVAL_BEFORE_LOGGING;
		this.timeBase = timeOfLastLogging;
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

				throw new RuntimeException("No source edge ID for " + getName(source) + "->" + getName(sink) + " "
					+ xored.toString());
			}

			EdgeCharacteristics edgeCharacteristics = profilingSubgraph
				.getEdgeCharacteristicsBySourceEdgeID(sourceEdgeID);

			edgeCharacteristics.addLatencyMeasurement(timestamp, channelLatency.getChannelLatency());
		}
	}

	private String getName(ExecutionVertex source) {
		String name = source.getName();
		for (int i = 0; i < source.getGroupVertex().getCurrentNumberOfGroupMembers(); i++) {
			if (source.getGroupVertex().getGroupMember(i) == source) {
				name += i;
				break;
			}
		}

		return name;
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

	public void logProfilingSummaryIfNecessary(long now) {
		if ((now - timeOfLastLogging) >= LOGGING_INTERVAL) {
			try {
				logger.logLatencies(now - timeBase);
			} catch (IOException e) {
				LOG.error("Error when writing to profiling logger file", e);
			}
			timeOfLastLogging = now;
		}
	}

}
