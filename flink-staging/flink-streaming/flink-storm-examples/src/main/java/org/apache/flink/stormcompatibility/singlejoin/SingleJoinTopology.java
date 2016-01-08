package org.apache.flink.stormcompatibility.singlejoin;

import backtype.storm.tuple.Fields;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.stormcompatibility.singlejoin.stormoperators.AgeSpout;
import org.apache.flink.stormcompatibility.singlejoin.stormoperators.GenderSpout;
import org.apache.flink.stormcompatibility.singlejoin.stormoperators.SingleJoinBolt;
import org.apache.flink.stormcompatibility.util.OutputFormatter;
import org.apache.flink.stormcompatibility.util.StormBoltFileSink;
import org.apache.flink.stormcompatibility.util.StormBoltPrintSink;
import org.apache.flink.stormcompatibility.util.TupleOutputFormatter;

public class SingleJoinTopology {

	public final static String spoutId1 = "gender";
	public final static String spoutId2 = "age";
	public final static String boltId = "singleJoin";
	public final static String sinkId = "sink";
	private final static OutputFormatter formatter = new TupleOutputFormatter();

	public static FlinkTopologyBuilder buildTopology() {

		final FlinkTopologyBuilder builder = new FlinkTopologyBuilder();

		// get input data
		builder.setSpout(spoutId1, new GenderSpout(new Fields("id", "gender")));
		builder.setSpout(spoutId2, new AgeSpout(new Fields("id", "age")));

		builder.setBolt(boltId, new SingleJoinBolt(new Fields("gender", "age")))
				.fieldsGrouping(spoutId1, new Fields("id"))
				.fieldsGrouping(spoutId2, new Fields("id"));
				//.shuffleGrouping(spoutId1)
				//.shuffleGrouping(spoutId2);

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new StormBoltFileSink(outputFile, formatter)).shuffleGrouping(boltId);
		} else {
			builder.setBolt(sinkId, new StormBoltPrintSink(formatter), 4).shuffleGrouping(boltId);
		}

		return builder;
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInputOutput = false;
	private static String outputPath;

	static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileInputOutput = true;
			if (args.length == 1) {
				outputPath = args[0];
			} else {
				System.err.println("Usage: StormSingleJoin* <result path>");
				return false;
			}
		} else {
			System.out.println("Executing StormSingleJoin* example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: StormSingleJoin* <result path>");
		}
		return true;
	}
}
