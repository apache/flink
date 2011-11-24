package eu.stratosphere.nephele.streaming.latency;

import java.util.ArrayList;
import java.util.Collections;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;

public class ProfilingSummary {

	ArrayList<ManagementAttachment> pathElements;

	int noOfActivePaths;
	
	int noOfInactivePaths;

	double avgTotalPathLatency;

	double medianPathLatency;

	double minPathLatency;

	double maxPathLatency;

	double[] avgPathElementLatencies;

	public ProfilingSummary(ProfilingSubgraph profilingSubgraph) {
		noOfActivePaths = 0;
		noOfInactivePaths = 0;
		maxPathLatency = Long.MIN_VALUE;
		minPathLatency = Long.MAX_VALUE;
		pathElements = null;
		avgPathElementLatencies = null;
		avgTotalPathLatency = 0;

		// will be sorted later on to determine the median
		ArrayList<Double> totalLatencies = new ArrayList<Double>();

		for (ProfilingPath path : profilingSubgraph.getProfilingPaths()) {
			ProfilingPathSummary pathSummary = path.getSummary();

			if (pathElements == null) {
				pathElements = pathSummary.getPathElements();
				avgPathElementLatencies = new double[pathElements.size()];
			}

			if (pathSummary.hasLatencies()) {
				// refresh the latency values in the summary
				pathSummary.refreshLatencies();

				avgTotalPathLatency += pathSummary.getTotalLatency();
				totalLatencies.add(pathSummary.getTotalLatency());

				// add the vertex/edge specific latency values to avgPathLatencies array
				addValues(pathSummary.getLatencies(), avgPathElementLatencies);

				noOfActivePaths++;
				maxPathLatency = Math.max(maxPathLatency, pathSummary.getTotalLatency());
				minPathLatency = Math.min(minPathLatency, pathSummary.getTotalLatency());
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
			medianPathLatency = totalLatencies.get(totalLatencies.size() / 2);
		} else {
			// set these back to zero because they have been set to very low/high values
			// initially
			maxPathLatency = 0;
			minPathLatency = 0;
		}
	}

	private void addValues(double[] from, double[] to) {
		for (int i = 0; i < from.length; i++) {
			to[i] += from[i];
		}
	}

}
