package eu.stratosphere.nephele.streaming.profiling;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class ProfilingLogger {

	private final static long WAIT_BEFORE_FIRST_LOGGING = 10 * 1000;

	private final static long LOGGING_INTERVAL = 1000;

	private BufferedWriter writer;

	private boolean headersWritten;

	private long timeOfNextLogging;

	private long timeBase;

	public ProfilingLogger()
			throws IOException {

		this.writer = new BufferedWriter(new FileWriter("profiling.txt"));
		this.headersWritten = false;
		this.timeOfNextLogging = ProfilingUtils.alignToNextFullSecond(System.currentTimeMillis() + WAIT_BEFORE_FIRST_LOGGING);
		this.timeBase = timeOfNextLogging;
	}

	public boolean isLoggingNecessary(long now) {
		return now >= timeOfNextLogging;
	}

	public void logLatencies(ProfilingSummary summary) throws IOException {
		long now = System.currentTimeMillis();
		long timestamp = now - timeBase;

		if (!headersWritten) {
			writeHeaders(summary);
		}

		StringBuilder builder = new StringBuilder();
		builder.append(timestamp);
		builder.append(';');
		builder.append(summary.getNoOfActivePaths());
		builder.append(';');
		builder.append(summary.getNoOfInactivePaths());
		builder.append(';');
		builder.append(summary.getAvgTotalPathLatency());
		builder.append(';');
		builder.append(summary.getMedianPathLatency());
		builder.append(';');
		builder.append(summary.getMinPathLatency());
		builder.append(';');
		builder.append(summary.getMaxPathLatency());

		for (double avgElementLatency : summary.getAvgPathElementLatencies()) {
			builder.append(';');
			builder.append(avgElementLatency);
		}
		builder.append('\n');
		writer.write(builder.toString());
		writer.flush(); // FIXME

		refreshTimeOfNextLogging();
	}

	private void refreshTimeOfNextLogging() {
		long now = System.currentTimeMillis();
		while(timeOfNextLogging <= now) {
			timeOfNextLogging += LOGGING_INTERVAL;
		}
	}

	private void writeHeaders(ProfilingSummary summary) throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("timestamp;");
		builder.append("noOfActivePaths;");
		builder.append("noOfInactivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("medianPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		int nextEdgeIndex = 1;
		for (ManagementAttachment element : summary.getPathElements()) {
			builder.append(';');
			if (element instanceof ManagementVertex) {
				ManagementVertex vertex = (ManagementVertex) element;
				builder.append(vertex.getGroupVertex().getName());
			} else {
				builder.append("edge" + nextEdgeIndex + "obl");
				builder.append(';');
				builder.append("edge" + nextEdgeIndex);
				nextEdgeIndex++;
			}
		}
		builder.append('\n');
		writer.write(builder.toString());
		headersWritten = true;
	}
}
