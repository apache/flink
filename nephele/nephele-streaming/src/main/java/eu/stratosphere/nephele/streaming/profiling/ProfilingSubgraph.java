package eu.stratosphere.nephele.streaming.profiling;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ManagementGraphFactory;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGraphIterator;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * This class offers a way to find, store and compute the latencies of all possible paths between to
 * {@link ExecutionGroupVertex} objects. Paths are computed on the {@link ExecutionVertex} level, not the
 * {@link ExecutionGroupVertex} level, hence there may be many paths for high degrees of parallelization.
 * 
 * @author Bjoern Lohrmann
 */
public class ProfilingSubgraph {

	// private static Log LOG = LogFactory.getLog(LatencySubgraph.class);

	private ManagementGroupVertex subgraphStart;

	private ManagementGroupVertex subgraphEnd;

	private List<ProfilingPath> profilingPaths;

	private HashMap<ManagementVertexID, VertexLatency> vertexLatencies = new HashMap<ManagementVertexID, VertexLatency>();

	private HashMap<ManagementEdgeID, EdgeCharacteristics> edgeCharacteristics = new HashMap<ManagementEdgeID, EdgeCharacteristics>();

	private HashMap<ManagementVertexID, ManagementEdgeID> receiverVertexToSourceEdgeIDMap = new HashMap<ManagementVertexID, ManagementEdgeID>();

	public ProfilingSubgraph(ExecutionGraph executionGraph, ExecutionGroupVertex subgraphStart,
			ExecutionGroupVertex subgraphEnd, boolean includeSubgraphStartInProfilingPaths,
			boolean includeSubgraphEndInProfilingPaths) {

		ManagementGraph managementGraph = ManagementGraphFactory.fromExecutionGraph(executionGraph);
		determineAnchoringManagementGroupVertices(managementGraph, subgraphStart, subgraphEnd);
		buildProfilingPaths(includeSubgraphStartInProfilingPaths, includeSubgraphEndInProfilingPaths);
		initProfilingAttachmentsOnPaths();
		initReceiverVertexToSourceEdgeIDMap(managementGraph);
	}

	private void initProfilingAttachmentsOnPaths() {
		for (ProfilingPath path : profilingPaths) {
			initProfilingAttachmentOnPath(path);
		}
	}

	private void initProfilingAttachmentOnPath(ProfilingPath path) {

		for (ManagementVertex vertex : path) {
			if (vertex.getAttachment() == null) {
				VertexLatency vertexLatency = new VertexLatency(vertex);
				vertex.setAttachment(vertexLatency);
				vertexLatencies.put(vertex.getID(), vertexLatency);
			}

			ManagementEdge ingoingEdge = path.getIngoingEdge(vertex);
			if (ingoingEdge != null && ingoingEdge.getAttachment() == null) {
				EdgeCharacteristics characteristics = new EdgeCharacteristics(ingoingEdge);
				ingoingEdge.setAttachment(characteristics);
				edgeCharacteristics.put(ingoingEdge.getSourceEdgeID(), characteristics);
				edgeCharacteristics.put(ingoingEdge.getTargetEdgeID(), characteristics);
			}
		}
	}

	private void initReceiverVertexToSourceEdgeIDMap(final ManagementGraph managementGraph) {

		final Iterator<ManagementVertex> it = new ManagementGraphIterator(managementGraph, true);
		while (it.hasNext()) {

			final ManagementVertex source = it.next();
			final int numberOfOutputGates = source.getNumberOfOutputGates();
			for (int i = 0; i < numberOfOutputGates; ++i) {
				final ManagementGate outputGate = source.getOutputGate(i);
				final int numberOfOutgoingEdges = outputGate.getNumberOfForwardEdges();
				for (int j = 0; j < numberOfOutgoingEdges; ++j) {
					final ManagementEdge edge = outputGate.getForwardEdge(j);
					final ManagementVertex receiver = edge.getTarget().getVertex();
					this.receiverVertexToSourceEdgeIDMap.put(receiver.getID(), edge.getSourceEdgeID());
				}
			}
		}
	}

	private void buildProfilingPaths(boolean includeSubgraphStartInProfilingPaths,
			boolean includeSubgraphEndInProfilingPaths) {
		
		this.profilingPaths = new LinkedList<ProfilingPath>();

		for (int i = 0; i < subgraphStart.getNumberOfGroupMembers(); i++) {
			ManagementVertex vertex = subgraphStart.getGroupMember(i);
			ProfilingPath initialPath = new ProfilingPath(this, vertex);
			depthFirstSearchProfilingPaths(initialPath, this.profilingPaths);
		}

		for (ProfilingPath profilingPath : profilingPaths) {
			profilingPath.setBeginVertexInProfilingPath(includeSubgraphStartInProfilingPaths);
			profilingPath.setEndVertexInProfilingPath(includeSubgraphEndInProfilingPaths);
		}
	}

	/**
	 * Performs a recursive depth first search for {@link #subgraphEnd} starting at the end of the given path.
	 * All paths found to end in {@link #subgraphEnd} are added to foundProfilingPaths.
	 * 
	 * @param path
	 *        Initial path with at least one element to start with (will be altered during recursive search).
	 * @param foundProfilingPaths
	 *        Accumulates the paths found to end at {@link #subgraphEnd}
	 */
	private void depthFirstSearchProfilingPaths(ProfilingPath path, List<ProfilingPath> foundProfilingPaths) {
		ManagementVertex pathEnd = path.getEndVertex();

		for (int i = 0; i < pathEnd.getNumberOfOutputGates(); i++) {
			ManagementGate outputGate = pathEnd.getOutputGate(i);

			for (int j = 0; j < outputGate.getNumberOfForwardEdges(); j++) {
				ManagementEdge edge = outputGate.getForwardEdge(j);

				ManagementVertex extension = edge.getTarget().getVertex();

				path.appendVertex(extension, edge);

				if (extension.getGroupVertex() == subgraphEnd) {
					foundProfilingPaths.add(new ProfilingPath(path));
				} else {
					depthFirstSearchProfilingPaths(path, foundProfilingPaths);
				}

				path.removeLastVertex();
			}
		}
	}

	private void determineAnchoringManagementGroupVertices(ManagementGraph managementGraph,
			ExecutionGroupVertex pathBeginExecVertex,
			ExecutionGroupVertex pathEndExecVertex) {

		ManagementVertexID vertexInPathBeginGroup = pathBeginExecVertex.getGroupMember(0).getID()
			.toManagementVertexID();
		this.subgraphStart = managementGraph.getVertexByID(vertexInPathBeginGroup).getGroupVertex();

		ManagementVertexID vertexInPathEndGroup = pathEndExecVertex.getGroupMember(0).getID().toManagementVertexID();
		this.subgraphEnd = managementGraph.getVertexByID(vertexInPathEndGroup).getGroupVertex();
	}

	public ManagementEdgeID getEdgeByReceiverVertexID(ManagementVertexID receiverVertexID) {
		return receiverVertexToSourceEdgeIDMap.get(receiverVertexID);
	}

	public EdgeCharacteristics getEdgeCharacteristicsBySourceEdgeID(ManagementEdgeID sourceEdgeID) {
		return edgeCharacteristics.get(sourceEdgeID);
	}

	public VertexLatency getVertexLatency(ManagementVertexID managementVertexID) {
		return vertexLatencies.get(managementVertexID);
	}

	public List<ProfilingPath> getProfilingPaths() {
		return profilingPaths;
	}

}
