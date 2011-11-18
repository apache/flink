package eu.stratosphere.nephele.streaming.latency;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * A latency path is a path through the ManagementGraph, defined by a sequence
 * of {@link ManagementVertex} objects that are connected in the order in which they appear in
 * the sequence.
 * 
 * @author Bjoern Lohrmann
 */
public class LatencyPath implements Iterable<ManagementVertex> {

	private LinkedList<ManagementVertex> pathVertices;

	private HashMap<ManagementVertexID, ManagementEdge> ingoingEdges;

	private LatencySubgraph graph;

	private long pathLatencyInMillis;

	@SuppressWarnings("unchecked")
	public LatencyPath(LatencyPath toClone) {
		this.graph = toClone.graph;
		this.pathVertices = (LinkedList<ManagementVertex>) toClone.pathVertices.clone();
		this.ingoingEdges = (HashMap<ManagementVertexID, ManagementEdge>) toClone.ingoingEdges.clone();
	}

	public LatencyPath(LatencySubgraph graph, ManagementVertex firstVertex) {
		this.graph = graph;
		this.pathVertices = new LinkedList<ManagementVertex>();
		this.ingoingEdges = new HashMap<ManagementVertexID, ManagementEdge>();
		this.pathVertices.add(firstVertex);
	}

	public void appendVertex(ManagementVertex vertex, ManagementEdge ingoingEdge) {
		pathVertices.add(vertex);
		ingoingEdges.put(vertex.getID(), ingoingEdge);
	}

	public ManagementVertex getBegin() {
		return pathVertices.getFirst();
	}

	public ManagementVertex getEnd() {
		return pathVertices.getFirst();
	}

	public ManagementEdge getIngoingEdge(ManagementVertex vertex) {
		return ingoingEdges.get(vertex.getID());
	}

	public void removeLastVertex() {
		ManagementVertex removed = pathVertices.removeLast();
		ingoingEdges.remove(removed);
	}

	@Override
	public Iterator<ManagementVertex> iterator() {
		return pathVertices.iterator();
	}

	public long refreshPathLatency() {
		this.pathLatencyInMillis = 0;
		for (ManagementVertex vertex : pathVertices) {
			ManagementEdge ingoingEdge = ingoingEdges.get(vertex.getID());

			if (ingoingEdge != null) {
				this.pathLatencyInMillis += ((EdgeLatency) ingoingEdge.getAttachment()).getLatencyInMillis();
			}
			this.pathLatencyInMillis += ((VertexLatency) vertex.getAttachment()).getLatencyInMillis();
		}
		return this.pathLatencyInMillis;
	}

	public long getPathLatencyInMillis() {
		return this.pathLatencyInMillis;
	}
}
