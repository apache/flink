package eu.stratosphere.nephele.streaming.latency;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * A profiling path is a path through the ManagementGraph, defined by a sequence
 * of sequentially connected {@link ManagementVertex} and {@link ManagementEdge} objects.
 * A profiling path may begin with a vertex or an edge an and may end with a vertex or an edge.
 * By default profiling paths begin and end with vertices, but this can be changed with
 * {{@link #setBeginVertexInProfilingPath(boolean)} and {{@link #setEndVertexInProfilingPath(boolean)}.
 * 
 * @author Bjoern Lohrmann
 */
public class ProfilingPath implements Iterable<ManagementVertex> {

	private LinkedList<ManagementVertex> pathVertices;

	private HashMap<ManagementVertexID, ManagementEdge> ingoingEdges;

	private ProfilingSubgraph graph;

	private ProfilingPathSummary summary;

	private boolean beginVertexInProfilingPath;

	private boolean endVertexInProfilingPath;

	@SuppressWarnings("unchecked")
	public ProfilingPath(ProfilingPath toClone) {
		this.graph = toClone.graph;
		this.pathVertices = (LinkedList<ManagementVertex>) toClone.pathVertices.clone();
		this.ingoingEdges = (HashMap<ManagementVertexID, ManagementEdge>) toClone.ingoingEdges.clone();
		this.beginVertexInProfilingPath = toClone.beginVertexInProfilingPath;
		this.endVertexInProfilingPath = toClone.endVertexInProfilingPath;
	}

	public ProfilingPath(ProfilingSubgraph graph, ManagementVertex firstVertex) {
		this.graph = graph;
		this.pathVertices = new LinkedList<ManagementVertex>();
		this.ingoingEdges = new HashMap<ManagementVertexID, ManagementEdge>();
		this.pathVertices.add(firstVertex);
		this.beginVertexInProfilingPath = true;
		this.endVertexInProfilingPath = true;
	}

	public void appendVertex(ManagementVertex vertex, ManagementEdge ingoingEdge) {
		pathVertices.add(vertex);
		ingoingEdges.put(vertex.getID(), ingoingEdge);
	}

	public ManagementVertex getBeginVertex() {
		return pathVertices.getFirst();
	}

	public ManagementVertex getEndVertex() {
		return pathVertices.getLast();
	}

	public void setBeginVertexInProfilingPath(boolean beginVertexInProfilingPath) {
		this.beginVertexInProfilingPath = beginVertexInProfilingPath;
	}

	public boolean isBeginVertexOnProfilingPath() {
		return beginVertexInProfilingPath;
	}

	public void setEndVertexInProfilingPath(boolean endVertexInProfilingPath) {
		this.endVertexInProfilingPath = endVertexInProfilingPath;
	}

	public boolean isEndVertexOnProfilingPath() {
		return endVertexInProfilingPath;
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

	public ProfilingPathSummary getSummary() {
		if (this.summary == null) {
			this.summary = new ProfilingPathSummary(walkProfilingPath());
		}
		return this.summary;
	}

	private ArrayList<ManagementAttachment> walkProfilingPath() {
		ArrayList<ManagementAttachment> profilingPathElements = new ArrayList<ManagementAttachment>();

		for (ManagementVertex vertex : pathVertices) {

			ManagementEdge ingoingEdge = ingoingEdges.get(vertex.getID());
			if (ingoingEdge != null) {
				profilingPathElements.add(ingoingEdge);
			}
			profilingPathElements.add(vertex);
		}

		if (!isBeginVertexOnProfilingPath()) {
			profilingPathElements.remove(0);
		}

		if (!isEndVertexOnProfilingPath()) {
			profilingPathElements.remove(profilingPathElements.size() - 1);
		}

		return profilingPathElements;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("LatencyPath[");
		ManagementVertex previous = null;
		for (ManagementVertex vertex : pathVertices) {
			if (previous != null) {
				builder.append("->");
			}
			builder.append(vertex);
			previous = vertex;
		}
		builder.append("]");

		return builder.toString();
	}

	// public void dumpLatencies() {
	//
	// for (ManagementVertex vertex : pathVertices) {
	// ManagementEdge ingoing = ingoingEdges.get(vertex.getID());
	//
	// if (ingoing != null) {
	// System.out.printf("---edge(%.03f)---%s(%.03f)\n",
	// ((EdgeCharacteristics) ingoing.getAttachment()).getLatencyInMillis(),
	// vertex,
	// ((VertexLatency) vertex.getAttachment()).getLatencyInMillis());
	// } else {
	// System.out.printf("%s(%.03f)\n", vertex,
	// ((VertexLatency) vertex.getAttachment()).getLatencyInMillis());
	// }
	// }
	// }
}
