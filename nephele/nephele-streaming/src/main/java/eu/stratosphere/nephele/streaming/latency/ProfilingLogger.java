package eu.stratosphere.nephele.streaming.latency;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class ProfilingLogger {

	private ProfilingSubgraph subgraph;

	private BufferedWriter writer;

	private boolean headersWritten;

	public ProfilingLogger(ProfilingSubgraph subgraph)
			throws IOException {

		this.subgraph = subgraph;
		this.writer = new BufferedWriter(new FileWriter("profiling.txt"));
		this.headersWritten = false;
	}

	public void logLatencies() throws IOException {
		ProfilingSummary summary = new ProfilingSummary(subgraph);
		if (!headersWritten) {
			writeHeaders(summary);
		}

		StringBuilder builder = new StringBuilder();
		builder.append(summary.noOfActivePaths);
		builder.append(';');
		builder.append(summary.noOfInactivePaths);
		builder.append(';');
		builder.append(summary.avgTotalPathLatency);
		builder.append(';');
		builder.append(summary.medianPathLatency);
		builder.append(';');
		builder.append(summary.minPathLatency);
		builder.append(';');
		builder.append(summary.maxPathLatency);

		for (double avgElementLatency : summary.avgPathElementLatencies) {
			builder.append(';');
			builder.append(avgElementLatency);
		}
		builder.append('\n');
		writer.write(builder.toString());
		writer.flush(); //FIXME
	}

	private void writeHeaders(ProfilingSummary summary) throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("noOfActivePaths;");
		builder.append("noOfInactivePaths;");
		builder.append("avgTotalPathLatency;");
		builder.append("medianPathLatency;");
		builder.append("minPathLatency;");
		builder.append("maxPathLatency");

		for (ManagementAttachment element : summary.pathElements) {
			builder.append(';');
			if (element instanceof ManagementVertex) {
				ManagementVertex vertex = (ManagementVertex) element;
				builder.append(vertex.getGroupVertex().getName());
			} else {
				builder.append("edge");
			}
		}
		builder.append('\n');
		writer.write(builder.toString());
		headersWritten = true;
	}
}
