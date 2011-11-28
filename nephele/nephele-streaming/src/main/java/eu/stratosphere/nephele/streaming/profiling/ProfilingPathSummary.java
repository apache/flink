package eu.stratosphere.nephele.streaming.profiling;

import java.util.ArrayList;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class ProfilingPathSummary {

	// private static Log LOG = LogFactory.getLog(ProfilingPathSummary.class);

	private double totalLatency;

	private double[] latencies;

	private boolean hasLatencies;

	private ArrayList<ManagementAttachment> pathElements;

	private int noOfPathElementLatencies;

	/**
	 * Initializes ProfilingPathLatency.
	 * 
	 * @param pathElements
	 *        Elements (vertices, edges, ..) of the path in the order in which they appear in the path.
	 */
	public ProfilingPathSummary(ArrayList<ManagementAttachment> pathElements) {
		this.pathElements = pathElements;
		this.noOfPathElementLatencies = countLatencyValuesOnPath(); 
		this.latencies = new double[noOfPathElementLatencies];
		this.hasLatencies = false;
		this.totalLatency = -1;
	}

	private int countLatencyValuesOnPath() {
		int valuesOnPath = 0;
		for (ManagementAttachment element : pathElements) {
			if (element instanceof ManagementVertex) {
				valuesOnPath++;
			} else {
				valuesOnPath += 2;
			}
		}
		return valuesOnPath;
	}

	/**
	 * Returns whether we have latency values for all elements (vertices and edges) of this
	 * path.
	 * 
	 * @return Whether we have latency values for all parts of this path
	 */
	public boolean hasLatencies() {
		if (!this.hasLatencies) {

			this.hasLatencies = true;
			for (ManagementAttachment element : pathElements) {

				if (element instanceof ManagementVertex) {
					VertexLatency vertexLatency = (VertexLatency) element.getAttachment();
					if (vertexLatency.getLatencyInMillis() == -1) {
						// ManagementEdge edge = (ManagementEdge) element;
						// String sourceName = edge.getSource().getVertex().getName() +
						// edge.getSource().getVertex().getIndexInGroup();
						// String tgName = edge.getTarget().getVertex().getName() +
						// edge.getTarget().getVertex().getIndexInGroup();
						// LOG.info("no data for edge " + sourceName + "-> " + tgName);
						this.hasLatencies = false;
						break;
					}
				} else {
					EdgeCharacteristics edgeChar = (EdgeCharacteristics) element.getAttachment();
					if (edgeChar.getChannelLatencyInMillis() == -1 ||
						edgeChar.getOutputBufferLatencyInMillis() == -1) {
						// ManagementVertex vertex = (ManagementVertex) element;
						// LOG.info("no data for vertex " + vertex.getName() + vertex.getIndexInGroup());
						this.hasLatencies = false;
						break;
					}
				}
			}
		}
		return hasLatencies;
	}

	public void refreshLatencies() {
		if (!hasLatencies()) {
			throw new UnsupportedOperationException(
				"Elements of profiling path do not have the necessary latency values yet");
		}

		this.totalLatency = 0;
		int index = 0;
		for (ManagementAttachment element : pathElements) {

			if (element instanceof ManagementVertex) {
				latencies[index] = ((VertexLatency) element.getAttachment()).getLatencyInMillis();
				this.totalLatency += latencies[index];
			} else {
				EdgeCharacteristics edgeCharacteristics = (EdgeCharacteristics) element.getAttachment();
				latencies[index] = edgeCharacteristics.getOutputBufferLatencyInMillis() / 2;

				if (latencies[index] < 0) {
					throw new RuntimeException(ProfilingUtils.formatName(element)
						+ " has invalid negative output buffer latency: " + latencies[index]);
				}

				index++;
				// channel latency includes output buffer latency, hence we subtract the output buffer latency
				// in order not to count it twice
				latencies[index] = Math.max(0, edgeCharacteristics.getChannelLatencyInMillis() - latencies[index - 1]);

				this.totalLatency += latencies[index] + latencies[index - 1];
			}
			index++;
		}
	}

	public double getTotalLatency() {
		return this.totalLatency;
	}

	public ArrayList<ManagementAttachment> getPathElements() {
		return pathElements;
	}

	public int getNoOfPathElementLatencies() {
		return noOfPathElementLatencies;
	}

	public double[] getPathElementLatencies() {
		return latencies;
	}

}
