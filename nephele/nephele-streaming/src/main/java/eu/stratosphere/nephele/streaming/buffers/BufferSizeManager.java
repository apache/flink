package eu.stratosphere.nephele.streaming.buffers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.streaming.StreamingJobManagerPlugin;
import eu.stratosphere.nephele.streaming.profiling.EdgeCharacteristics;
import eu.stratosphere.nephele.streaming.profiling.ProfilingModel;
import eu.stratosphere.nephele.streaming.profiling.ProfilingPath;
import eu.stratosphere.nephele.streaming.profiling.ProfilingSummary;
import eu.stratosphere.nephele.streaming.profiling.ProfilingUtils;
import eu.stratosphere.nephele.taskmanager.bufferprovider.GlobalBufferPool;

public class BufferSizeManager {

	private final static long WAIT_BEFORE_FIRST_ADJUSTMENT = 30 * 1000;

	public final static long ADJUSTMENT_INTERVAL = 10 * 1000;

	private Log LOG = LogFactory.getLog(BufferSizeManager.class);

	private long latencyGoal;

	private ProfilingModel profilingModel;

	private StreamingJobManagerPlugin jobManagerPlugin;

	private ExecutionGraph executionGraph;

	private HashMap<ManagementEdge, BufferSizeHistory> bufferSizes;

	private long timeOfNextAdjustment;

	public BufferSizeManager(long latencyGoal, ProfilingModel profilingModel,
			StreamingJobManagerPlugin jobManagerPlugin,
			ExecutionGraph executionGraph) {
		this.latencyGoal = latencyGoal;
		this.profilingModel = profilingModel;
		this.jobManagerPlugin = jobManagerPlugin;
		this.executionGraph = executionGraph;
		this.bufferSizes = new HashMap<ManagementEdge, BufferSizeHistory>();
		this.timeOfNextAdjustment = ProfilingUtils.alignToNextFullSecond(System.currentTimeMillis()
			+ WAIT_BEFORE_FIRST_ADJUSTMENT);
		initBufferSizes();
	}

	private void initBufferSizes() {
		int bufferSize = GlobalConfiguration.getInteger("channel.network.bufferSizeInBytes",
			GlobalBufferPool.DEFAULT_BUFFER_SIZE_IN_BYTES);

		long now = System.currentTimeMillis();
		for (ProfilingPath path : profilingModel.getProfilingSubgraph().getProfilingPaths()) {
			for (ManagementAttachment pathElement : path.getPathElements()) {
				if (pathElement instanceof ManagementEdge) {
					ManagementEdge edge = (ManagementEdge) pathElement;
					BufferSizeHistory bufferSizeHistory = new BufferSizeHistory(edge, 2);
					bufferSizeHistory.addToHistory(now, bufferSize);
					bufferSizes.put(edge, bufferSizeHistory);
				}
			}
		}
	}

	public void adjustBufferSizes(ProfilingSummary summary) {
		HashMap<ManagementEdge, Integer> edgesToAdjust = new HashMap<ManagementEdge, Integer>();

		for (ProfilingPath activePath : summary.getActivePaths()) {
			if (activePath.getSummary().getTotalLatency() > latencyGoal) {
				collectEdgesToAdjust(activePath, edgesToAdjust);
			}
		}

		doAdjust(edgesToAdjust);

		refreshTimeOfNextAdjustment();
	}

	private void doAdjust(HashMap<ManagementEdge, Integer> edgesToAdjust) {

		for (ManagementEdge edge : edgesToAdjust.keySet()) {
			int newBufferSize = edgesToAdjust.get(edge);

			BufferSizeHistory sizeHistory = bufferSizes.get(edge);

			LOG.info(String.format("New buffer size: %s new: %d (old: %d)", ProfilingUtils.formatName(edge),
				newBufferSize, sizeHistory.getLastEntry().getBufferSize()));

			setBufferSize(edge.getSourceEdgeID(), newBufferSize);

			sizeHistory.addToHistory(timeOfNextAdjustment, newBufferSize);
		}
	}

	private void refreshTimeOfNextAdjustment() {
		long now = System.currentTimeMillis();
		while (timeOfNextAdjustment <= now) {
			timeOfNextAdjustment += ADJUSTMENT_INTERVAL;
		}
	}

	ArrayList<ManagementEdge> edgesSortedByLatency = new ArrayList<ManagementEdge>();

	Comparator<ManagementEdge> edgeComparator = new Comparator<ManagementEdge>() {
		@Override
		public int compare(ManagementEdge first, ManagementEdge second) {
			double firstLatency = ((EdgeCharacteristics) first.getAttachment()).getChannelLatencyInMillis();
			double secondLatency = ((EdgeCharacteristics) second.getAttachment()).getChannelLatencyInMillis();

			if (firstLatency < secondLatency) {
				return -1;
			} else if (firstLatency > secondLatency) {
				return 1;
			} else {
				return 0;
			}
		}
	};

	private void collectEdgesToAdjust(ProfilingPath path, HashMap<ManagementEdge, Integer> edgesToAdjust) {
		for (ManagementAttachment element : path.getPathElements()) {
			if (element instanceof ManagementEdge) {
				edgesSortedByLatency.add((ManagementEdge) element);
			}
		}

		Collections.sort(edgesSortedByLatency, edgeComparator);

		for (ManagementEdge edge : edgesSortedByLatency) {

			if (edgesToAdjust.containsKey(edge)) {
				continue;
			}

			EdgeCharacteristics edgeChar = (EdgeCharacteristics) edge.getAttachment();

			if (!hasFreshValues(edge)) {
//				LOG.info("Rejecting edge due to stale values: " + ProfilingUtils.formatName(edge));
				continue;
			}

			double edgeLatency = edgeChar.getChannelLatencyInMillis();
			double avgOutputBufferLatency = edgeChar.getOutputBufferLatencyInMillis() / 2;

			if (avgOutputBufferLatency > 5 && avgOutputBufferLatency >= 0.05 * edgeLatency) {
				reduceBufferSize(edge, edgesToAdjust);
			} else if (avgOutputBufferLatency <= 1) {
				increaseBufferSize(edge, edgesToAdjust);
			}
		}

		edgesSortedByLatency.clear();
	}

	private void increaseBufferSize(ManagementEdge edge, HashMap<ManagementEdge, Integer> edgesToAdjust) {
		int oldBufferSize = bufferSizes.get(edge).getLastEntry().getBufferSize();
		int newBufferSize = proposedIncreasedBufferSize(oldBufferSize);

		edgesToAdjust.put(edge, newBufferSize);
	}

	private int proposedIncreasedBufferSize(int oldBufferSize) {
		return (int) (oldBufferSize * 1.2);
	}

	private void reduceBufferSize(ManagementEdge edge, HashMap<ManagementEdge, Integer> edgesToAdjust) {
		int oldBufferSize = bufferSizes.get(edge).getLastEntry().getBufferSize();
		int newBufferSize = proposedReducedBufferSize(edge, oldBufferSize);

		// filters pointless minor changes in buffer size
		if (isRelevantReduction(newBufferSize, oldBufferSize)) {
			edgesToAdjust.put(edge, newBufferSize);
		}
		
//		else {
//			LOG.info(String.format("Filtering reduction due to insignificance: %s (old:%d new:%d)",
//				ProfilingUtils.formatName(edge), oldBufferSize, newBufferSize));
//		}
	}

	private boolean isRelevantReduction(int newBufferSize, int oldBufferSize) {
		return newBufferSize < oldBufferSize * 0.98;
	}

	private int proposedReducedBufferSize(ManagementEdge edge, int oldBufferSize) {
		EdgeCharacteristics edgeChar = (EdgeCharacteristics) edge.getAttachment();

		double avgOutputBufferLatency = edgeChar.getOutputBufferLatencyInMillis() / 2;

		double reductionFactor = Math.pow(0.99, avgOutputBufferLatency);
		reductionFactor = Math.max(0.1, reductionFactor);

		int newBufferSize = (int) Math.max(100, oldBufferSize * reductionFactor);

		return newBufferSize;
	}

	private boolean hasFreshValues(ManagementEdge edge) {
		EdgeCharacteristics edgeChar = (EdgeCharacteristics) edge.getAttachment();
		long freshnessThreshold = bufferSizes.get(edge).getLastEntry().getTimestamp();

		return edgeChar.isChannelLatencyFresherThan(freshnessThreshold)
			&& edgeChar.isOutputBufferLatencyFresherThan(freshnessThreshold);
	}

	public boolean isAdjustmentNecessary(long now) {
		return now >= timeOfNextAdjustment;
	}

	private void setBufferSize(ManagementEdgeID sourceEdgeID, int bufferSize) {
		ChannelID sourceChannelID = sourceEdgeID.toChannelID();
		ExecutionVertex vertex = this.executionGraph.getVertexByChannelID(sourceChannelID);
		if (vertex == null) {
			LOG.error("Cannot find vertex to channel ID " + vertex);
			return;
		}
		this.jobManagerPlugin.limitBufferSize(vertex, sourceChannelID, bufferSize);
	}
}
