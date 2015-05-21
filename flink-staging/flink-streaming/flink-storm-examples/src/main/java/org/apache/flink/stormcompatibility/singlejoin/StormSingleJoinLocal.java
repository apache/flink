package org.apache.flink.stormcompatibility.singlejoin;

import backtype.storm.utils.Utils;
import org.apache.flink.stormcompatibility.api.FlinkLocalCluster;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.stormcompatibility.wordcount.WordCountTopology;

public class StormSingleJoinLocal {
	public final static String topologyId = "Streaming SingleJoin";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!SingleJoinTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final FlinkTopologyBuilder builder = SingleJoinTopology.buildTopology();

		// execute program locally
		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology(topologyId, null, builder.createTopology());

		Utils.sleep(5 * 1000);

		// TODO kill does no do anything so far
		cluster.killTopology(topologyId);
		cluster.shutdown();
	}
}
