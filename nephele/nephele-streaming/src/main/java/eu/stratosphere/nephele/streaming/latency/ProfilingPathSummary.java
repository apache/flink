package eu.stratosphere.nephele.streaming.latency;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class ProfilingPathSummary {

	private double totalLatency;

	private double[] latencies;

	private boolean hasLatencies;

	private ArrayList<ManagementAttachment> pathElements;

	/**
	 * Initializes ProfilingPathLatency.
	 * 
	 * @param pathElements
	 *        Elements (vertices, edges, ..) of the path in the order in which they appear in the path.
	 */
	public ProfilingPathSummary(ArrayList<ManagementAttachment> pathElements) {
		this.pathElements = pathElements;
		this.latencies = new double[pathElements.size()];
		this.hasLatencies = false;
		this.totalLatency = -1;
	}

	private class LatencyPathEntry implements Entry<ManagementAttachment, Double> {
		ManagementAttachment key;

		double value;

		@Override
		public ManagementAttachment getKey() {
			return key;
		}

		@Override
		public Double getValue() {
			return value;
		}

		@Override
		public Double setValue(Double value) {
			throw new UnsupportedOperationException();
		}
	}

	public Iterable<Entry<ManagementAttachment, Double>> getLatencyIterable() {
		return new Iterable<Entry<ManagementAttachment, Double>>() {
			@Override
			public Iterator<Entry<ManagementAttachment, Double>> iterator() {
				return new Iterator<Entry<ManagementAttachment, Double>>() {
					int index = 0;

					LatencyPathEntry entry = new LatencyPathEntry();

					@Override
					public boolean hasNext() {
						return index < latencies.length;
					}

					@Override
					public Entry<ManagementAttachment, Double> next() {
						entry.key = pathElements.get(index);
						entry.value = latencies[index];
						return entry;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
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

				if ((element instanceof ManagementVertex && ((VertexLatency) element.getAttachment())
					.getLatencyInMillis() == -1)
					|| (element instanceof ManagementEdge && ((EdgeCharacteristics) element.getAttachment())
						.getLatencyInMillis() == -1)) {
					this.hasLatencies = false;
					break;
				}
			}
		}

		return hasLatencies;
	}

	public void refreshLatencies() {
		if (!hasLatencies()) {
			throw new UnsupportedOperationException(
				"Elements of profiling path does not have all do not have the necessary latency values yet");
		}

		this.totalLatency = 0;
		int index = 0;
		for (ManagementAttachment managementAttachment : pathElements) {
			double latency;

			if (managementAttachment instanceof ManagementVertex) {
				latency = ((VertexLatency) managementAttachment.getAttachment()).getLatencyInMillis();
			} else {
				latency = ((EdgeCharacteristics) managementAttachment.getAttachment()).getLatencyInMillis();
			}

			latencies[index] = latency;
			this.totalLatency += latency;
			index++;
		}
	}
	
	public double getTotalLatency() {
		return this.totalLatency;
	}

	public ArrayList<ManagementAttachment> getPathElements() {
		return pathElements;
	}

	public double[] getLatencies() {
		return latencies;
	}

}
