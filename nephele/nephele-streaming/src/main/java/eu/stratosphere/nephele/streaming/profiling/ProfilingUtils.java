package eu.stratosphere.nephele.streaming.profiling;

import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.managementgraph.ManagementAttachment;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class ProfilingUtils {

	public static String formatName(ManagementAttachment managementAttachment) {
		if (managementAttachment instanceof ManagementVertex) {
			return formatName((ManagementVertex) managementAttachment);
		} else {
			return formatName((ManagementEdge) managementAttachment);
		}
	}

	public static String formatName(ManagementEdge edge) {
		return formatName(edge.getSource().getVertex()) + "->" + formatName(edge.getTarget().getVertex());
	}

	public static String formatName(ManagementVertex vertex) {
		String name = vertex.getName();
		for (int i = 0; i < vertex.getGroupVertex().getNumberOfGroupMembers(); i++) {
			if (vertex.getGroupVertex().getGroupMember(i) == vertex) {
				name += i;
				break;
			}
		}
		return name;
	}

	public static String formatName(ExecutionVertex vertex) {
		String name = vertex.getName();
		for (int i = 0; i < vertex.getGroupVertex().getCurrentNumberOfGroupMembers(); i++) {
			if (vertex.getGroupVertex().getGroupMember(i) == vertex) {
				name += i;
				break;
			}
		}
		return name;
	}

	public static long alignToNextFullSecond(long timestampInMillis) {
		long remainder = timestampInMillis % 1000;

		if (remainder > 0) {
			return timestampInMillis - remainder + 1000;
		}
		return timestampInMillis;
	}
}
