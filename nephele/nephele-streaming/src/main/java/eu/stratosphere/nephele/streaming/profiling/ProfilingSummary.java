package eu.stratosphere.nephele.streaming.profiling;

import java.util.ArrayList;
import java.util.Collections;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;

public class ProfilingSummary {

	private ArrayList<ManagementAttachment> pathElements;

	private int noOfActivePaths;

	private int noOfInactivePaths;

	private double avgTotalPathLatency;

	private double medianPathLatency;

	private double minPathLatency;

	private double maxPathLatency;

	private double[] avgPathElementLatencies;
	
	private ArrayList<ProfilingPath> activePaths;

	public ProfilingSummary(ProfilingSubgraph profilingSubgraph) {
		noOfActivePaths = 0;
		noOfInactivePaths = 0;
		avgTotalPathLatency = 0;
		minPathLatency = 0;
		maxPathLatency = 0;
		medianPathLatency = 0;
		activePaths = new ArrayList<ProfilingPath>();
		
		pathElements = null;
		avgPathElementLatencies = null;
		
		// will be sorted later on to determine min, max and median
		ArrayList<Double> totalLatencies = new ArrayList<Double>();

		for (ProfilingPath path : profilingSubgraph.getProfilingPaths()) {
			ProfilingPathSummary pathSummary = path.getSummary();

			if (pathElements == null) {
				pathElements = pathSummary.getPathElements();
				avgPathElementLatencies = new double[pathSummary.getNoOfPathElementLatencies()];
			}

			if (pathSummary.hasLatencies()) {
				activePaths.add(path);
				
				// refresh the latency values in the summary
				pathSummary.refreshLatencies();

				avgTotalPathLatency += pathSummary.getTotalLatency();
				totalLatencies.add(pathSummary.getTotalLatency());

				// add the vertex/edge specific latency values to avgPathLatencies array
				addValues(pathSummary.getPathElementLatencies(), avgPathElementLatencies);

				noOfActivePaths++;
			} else {
				noOfInactivePaths++;
			}
		}

		if (noOfActivePaths > 0) {		
			for (int i = 0; i < avgPathElementLatencies.length; i++) {
				avgPathElementLatencies[i] = avgPathElementLatencies[i] / noOfActivePaths;
			}

			avgTotalPathLatency = avgTotalPathLatency / noOfActivePaths;

			Collections.sort(totalLatencies);
			minPathLatency = totalLatencies.get(0);
			medianPathLatency = totalLatencies.get(totalLatencies.size() / 2);
			maxPathLatency = totalLatencies.get(totalLatencies.size() -1);
		}
	}

	private void addValues(double[] from, double[] to) {
		for (int i = 0; i < from.length; i++) {
			to[i] += from[i];
		}
	}

	public ArrayList<ManagementAttachment> getPathElements() {
		return pathElements;
	}

	public int getNoOfActivePaths() {
		return noOfActivePaths;
	}

	public int getNoOfInactivePaths() {
		return noOfInactivePaths;
	}

	public double getAvgTotalPathLatency() {
		return avgTotalPathLatency;
	}

	public double getMedianPathLatency() {
		return medianPathLatency;
	}

	public double getMinPathLatency() {
		return minPathLatency;
	}

	public double getMaxPathLatency() {
		return maxPathLatency;
	}

	public double[] getAvgPathElementLatencies() {
		return avgPathElementLatencies;
	}

	public ArrayList<ProfilingPath> getActivePaths() {
		return activePaths;
	}
}
