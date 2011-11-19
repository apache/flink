package eu.stratosphere.nephele.streaming.latency;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ManagementGraphFactory;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
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
public class LatencySubgraph {
	
	private static Log LOG = LogFactory.getLog(LatencySubgraph.class);

	private ManagementGroupVertex subgraphStart;

	private ManagementGroupVertex subgraphEnd;

	private List<LatencyPath> latencyPaths;

	private HashMap<ManagementVertexID, VertexLatency> vertexLatencies = new HashMap<ManagementVertexID, VertexLatency>();

	private HashMap<ManagementEdgeID, EdgeLatency> edgeLatencies = new HashMap<ManagementEdgeID, EdgeLatency>();

	public LatencySubgraph(ExecutionGraph executionGraph, ExecutionGroupVertex subgraphStart,
			ExecutionGroupVertex subgraphEnd) {

		ManagementGraph managementGraph = ManagementGraphFactory.fromExecutionGraph(executionGraph);
		determineAnchoringManagementGroupVertices(managementGraph, subgraphStart, subgraphEnd);
		buildLatencyPaths();
		initLatenciesOnPaths();
	}

	private void initLatenciesOnPaths() {
		for (LatencyPath path : latencyPaths) {
			initLatenciesOnPath(path);
		}
	}

	private void initLatenciesOnPath(LatencyPath path) {

		for (ManagementVertex vertex : path) {
			if (vertex.getAttachment() == null) {
				VertexLatency vertexLatency = new VertexLatency(vertex);
				vertex.setAttachment(vertexLatency);
				vertexLatencies.put(vertex.getID(), vertexLatency);
			}

			ManagementEdge ingoingEdge = path.getIngoingEdge(vertex);
			if (ingoingEdge != null && ingoingEdge.getAttachment() == null) {
				EdgeLatency edgeLatency = new EdgeLatency(ingoingEdge);
				ingoingEdge.setAttachment(edgeLatency);
				edgeLatencies.put(ingoingEdge.getManagementEdgeID(), edgeLatency);
			}
		}
	}

	private void buildLatencyPaths() {
		this.latencyPaths = new LinkedList<LatencyPath>();

		for (int i = 0; i < subgraphStart.getNumberOfGroupMembers(); i++) {
			ManagementVertex vertex = subgraphStart.getGroupMember(i);
			LatencyPath initialPath = new LatencyPath(this, vertex);
			depthFirstSearchLatencyPaths(initialPath, this.latencyPaths);
		}
	}

	/**
	 * Performs a recursive depth first search for {@link #subgraphEnd} starting at the end of the given path.
	 * All paths found to end in {@link #subgraphEnd} are added to foundLatencyPaths.
	 * 
	 * @param path
	 *        Initial path with at least one element to start with (will be altered during recursive search).
	 * @param foundLatencyPaths
	 *        Accumulates the paths found to end at {@link #subgraphEnd}
	 */
	private void depthFirstSearchLatencyPaths(LatencyPath path, List<LatencyPath> foundLatencyPaths) {
		ManagementVertex pathEnd = path.getEnd();

		for (int i = 0; i < pathEnd.getNumberOfOutputGates(); i++) {
			ManagementGate outputGate = pathEnd.getOutputGate(i);

			for (int j = 0; j < outputGate.getNumberOfForwardEdges(); j++) {
				ManagementEdge edge = outputGate.getForwardEdge(j);

				ManagementVertex extension = edge.getTarget().getVertex();

				path.appendVertex(extension, edge);

				if (extension.getGroupVertex() == subgraphEnd) {
					foundLatencyPaths.add(new LatencyPath(path));
				} else {
					depthFirstSearchLatencyPaths(path, foundLatencyPaths);
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

	public EdgeLatency getEdgeLatency(ManagementEdgeID edgeID) {
		return edgeLatencies.get(edgeID);
	}
	
	public VertexLatency getVertexLatency(ManagementVertexID managementVertexID) {
		return vertexLatencies.get(managementVertexID);
	}
	
	public List<LatencyPath> getLatencyPaths() {
		return latencyPaths;
	}

}
