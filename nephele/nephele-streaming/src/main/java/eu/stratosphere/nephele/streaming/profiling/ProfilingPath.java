package eu.stratosphere.nephele.streaming.profiling;

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

	private ProfilingSubgraph graph;

	private LinkedList<ManagementVertex> pathVertices;

	private HashMap<ManagementVertexID, ManagementEdge> ingoingEdges;

	private ArrayList<ManagementAttachment> pathElements;

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

	public ProfilingPath(ProfilingSubgraph graph, ManagementVertex firstVertex, boolean beginVertexInProfilingPath,
			boolean endVertexInProfilingPath) {
		this.graph = graph;
		this.pathVertices = new LinkedList<ManagementVertex>();
		this.ingoingEdges = new HashMap<ManagementVertexID, ManagementEdge>();
		this.pathVertices.add(firstVertex);
		this.beginVertexInProfilingPath = beginVertexInProfilingPath;
		this.endVertexInProfilingPath = endVertexInProfilingPath;
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
		// changing this on the fly will invalidate any already computed list of path elements
		// and the summary
		if (beginVertexInProfilingPath != this.beginVertexInProfilingPath) {
			this.pathElements = null;
			this.summary = null;
		}

		this.beginVertexInProfilingPath = beginVertexInProfilingPath;
	}

	public boolean isBeginVertexOnProfilingPath() {
		return beginVertexInProfilingPath;
	}

	public void setEndVertexInProfilingPath(boolean endVertexInProfilingPath) {
		// changing this on the fly will invalidate any already computed list of path elements
		// and the summary
		if (endVertexInProfilingPath != this.endVertexInProfilingPath) {
			this.pathElements = null;
			this.summary = null;
		}

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
		ensurePathSummaryInitialized();
		return this.summary;
	}

	private void ensurePathSummaryInitialized() {
		if (this.summary == null) {
			ensurePathElementsInitialized();
			this.summary = new ProfilingPathSummary(this.pathElements);
		}
	}

	private void ensurePathElementsInitialized() {
		if (this.pathElements == null) {
			this.pathElements = walkProfilingPath();
		}
	}

	private ArrayList<ManagementAttachment> walkProfilingPath() {
		ArrayList<ManagementAttachment> pathElements = new ArrayList<ManagementAttachment>();

		for (ManagementVertex vertex : pathVertices) {

			ManagementEdge ingoingEdge = ingoingEdges.get(vertex.getID());
			if (ingoingEdge != null) {
				pathElements.add(ingoingEdge);
			}
			pathElements.add(vertex);
		}

		if (!isBeginVertexOnProfilingPath()) {
			pathElements.remove(0);
		}

		if (!isEndVertexOnProfilingPath()) {
			pathElements.remove(pathElements.size() - 1);
		}

		return pathElements;
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

	public ArrayList<ManagementAttachment> getPathElements() {
		ensurePathElementsInitialized();
		return this.pathElements;
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
