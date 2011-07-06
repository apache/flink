/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.jobgen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.PactConnection.TempMode;

/**
 * Translator for @see eu.stratosphere.pact.compiler.plan.OptimizedPlan into a JSON representation.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class JSONGenerator implements Visitor<OptimizerNode> {

	// ------------------------------------------------------------------------
	// Members
	// ------------------------------------------------------------------------

	private Hashtable<OptimizerNode, Integer> nodeIds; // resolves pact nodes to ids

	int nodeCnt; // resolves pact nodes to ids

	StringBuffer jsonString; // builds the json string

	/**
	 * Generates a JSON representation from the @see eu.stratosphere.pact.compiler.plan.OptimizedPlan
	 * and writes it to a file.
	 * 
	 * @param plan
	 *        Plan that is translated to JSON
	 * @param jsonFile
	 *        File to which the JSON output is written
	 * @throws IOException
	 */
	public void writeJSONFile(OptimizedPlan plan, File jsonFile) throws IOException {
		FileWriter fw;

		fw = new FileWriter(jsonFile);
		fw.write(compilePlanToJSON(plan));
		fw.close();
	}

	/**
	 * Translates a @see eu.stratosphere.pact.compiler.plan.OptimizedPlan in a JSON representation.
	 * 
	 * @param plan
	 *        Plan that is translated to JSON
	 * @return The JSON representation of the plan
	 */
	public String compilePlanToJSON(OptimizedPlan plan) {

		// initialization to assign node ids
		this.nodeIds = new Hashtable<OptimizerNode, Integer>();
		this.nodeCnt = 0;

		// init JSON string builder
		this.jsonString = new StringBuffer();

		// JSON header
		this.jsonString.append("{\n\t\"nodes\": [\n\n");

		// Generate JSON for plan
		plan.accept(this);

		// JSON Footer
		jsonString.deleteCharAt(jsonString.lastIndexOf(","));
		jsonString.append("\t]\n}");

		// return JSON string
		return jsonString.toString();
	}

	@Override
	public boolean preVisit(OptimizerNode visitable) {

		// visit each node and assign a unique id
		if (!nodeIds.containsKey(visitable)) {
			nodeIds.put(visitable, nodeCnt++);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void postVisit(OptimizerNode visitable) {

		// start a new node
		jsonString.append("\t{\n");

		// output node id
		jsonString.append("\t\t\"id\": " + nodeIds.get(visitable));

		// output node type
		String type;
		switch (visitable.getPactType()) {
		case DataSink:
			type = "sink";
			break;
		case DataSource:
			type = "source";
			break;
		default:
			type = "pact";
			break;
		}
		jsonString.append(",\n\t\t\"type\": \"" + type + "\"");

		// output node pact type
		if (type.equals("pact")) {
			jsonString.append(",\n\t\t\"pact\": \"" + visitable.getName() + "\"");
		}

		// output node contents
		String contents;
		switch (visitable.getPactType()) {
		case DataSink:
			contents = ((FileDataSinkContract<?, ?>) visitable.getPactContract()).getFilePath();
			break;
		case DataSource:
			contents = ((FileDataSourceContract<?, ?>) visitable.getPactContract()).getFilePath();
			break;
		default:
			contents = visitable.getPactContract().getName();
			break;
		}
		jsonString.append(",\n\t\t\"contents\": \"" + contents + "\"");

		// output contract
		OutputContract outContr = visitable.getOutputContract();
		if (outContr != null && outContr != OutputContract.None) {
			jsonString.append(",\n\t\t\"outputcontract\": \"" + outContr.name() + "\"");
		}

		// degree of parallelism
		jsonString.append(",\n\t\t\"parallelism\": \""
			+ (visitable.getDegreeOfParallelism() >= 1 ? visitable.getDegreeOfParallelism() : "default") + "\"");

		// output node predecessors
		List<PactConnection> connList = visitable.getIncomingConnections();
		String child1name = null, child2name = null;

		if (connList != null && connList.size() > 0) {
			// start predecessor list
			jsonString.append(",\n\t\t\"predecessors\": [");
			int connCnt = 0;
			for (PactConnection conn : connList) {

				jsonString.append(connCnt == 0 ? "\n" : ",\n");
				if (connCnt == 0) {
					child1name = conn.getSourcePact().getPactContract().getName();
				} else if (connCnt == 1) {
					child2name = conn.getSourcePact().getPactContract().getName();
				}

				// output predecessor id
				jsonString.append("\t\t\t{\"id\": " + nodeIds.get(conn.getSourcePact()));

				// output connection side
				if (connList.size() == 2) {
					jsonString.append(", \"side\": \"" + (connCnt == 0 ? "first" : "second") + "\"");
				}
				// output shipping strategy and channel type
				String shipStrategy = null;
				String channelType = null;
				switch (conn.getShipStrategy()) {
				case NONE:
					// nothing
					break;
				case FORWARD:
					shipStrategy = "Local Forward";
					channelType = "memory";
					break;
				case BROADCAST:
					shipStrategy = "Broadcast";
					channelType = "network";
					break;
				case PARTITION_HASH:
					shipStrategy = "Partition";
					channelType = "network";
					break;
				case PARTITION_RANGE:
					shipStrategy = "Partition (range)";
					channelType = "network";
					break;
				case PARTITION_LOCAL_HASH:
					shipStrategy = "Partition local";
					channelType = "memory";
				case SFR:
					shipStrategy = "SFR";
					channelType = "network";
					break;
				default:
					throw new CompilerException("Unknown ship strategy '" + conn.getShipStrategy().name()
						+ "' in JSON generator.");
				}

				if (shipStrategy != null) {
					jsonString.append(", \"shippingStrategy\": \"" + shipStrategy + "\"");
				}
				if (channelType != null) {
					jsonString.append(", \"channelType\": \"" + channelType + "\"");
				}

				if (conn.getTempMode() != TempMode.NONE) {
					String tempMode = conn.getTempMode().toString();
					jsonString.append(", \"tempMode\": \"" + tempMode + "\"");
				}

				jsonString.append('}');

				connCnt++;
			}
			// finish predecessors
			jsonString.append("\t\t]");
		}

		// local strategy
		String locString = null;
		if (visitable.getLocalStrategy() != null) {
			switch (visitable.getLocalStrategy()) {
			case NONE:
				// nothing
				break;
			case HYBRIDHASH_FIRST:
				locString = "Hybrid Hash (build: " + child1name + ")";
				break;
			case HYBRIDHASH_SECOND:
				locString = "Hybrid Hash (build: " + child2name + ")";
				break;
			case MMHASH_FIRST:
				locString = "Main-Memory Hash (build: " + child1name + ")";
				break;
			case MMHASH_SECOND:
				locString = "Main-Memory Hash (build: " + child2name + ")";
				break;
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:
				locString = "Nested Loops (Blocked Outer: " + child1name + ")";
				break;
			case NESTEDLOOP_BLOCKED_OUTER_SECOND:
				locString = "Nested Loops (Blocked Outer: " + child2name + ")";
				break;
			case NESTEDLOOP_STREAMED_OUTER_FIRST:
				locString = "Nested Loops (Streamed Outer: " + child1name + ")";
				break;
			case NESTEDLOOP_STREAMED_OUTER_SECOND:
				locString = "Nested Loops (Streamed Outer: " + child2name + ")";
				break;
			case SORT_BOTH_MERGE:
				locString = "Sort-Both-Merge";
				break;
			case SORT_FIRST_MERGE:
				locString = "Sort-First-Merge";
				break;
			case SORT_SECOND_MERGE:
				locString = "Sort-Second-Merge";
				break;
			case MERGE:
				locString = "Merge";
				break;
			case SORT:
				locString = "Sort";
				break;
			case COMBININGSORT:
				locString = "Sort with Combiner";
				break;
			case SORT_SELF_NESTEDLOOP:
				locString = "Sort Self-Nested-Loops";
				break;
			case SELF_NESTEDLOOP:
				locString = "Self-Nested-Loops";
				break;
			default:
				throw new CompilerException("Unknown local strategy '" + visitable.getLocalStrategy().name()
					+ "' in JSON generator.");
			}

			if (locString != null) {
				jsonString.append(",\n\t\t\"local_strategy\": \"");
				jsonString.append(locString);
				jsonString.append("\"");
			}
		}

		{
			// output node global properties
			GlobalProperties gp = visitable.getGlobalProperties();

			jsonString.append(",\n\t\t\"global_properties\": [\n");

			addProperty(jsonString, "Key-Partitioning", gp.getPartitioning().name(), true);
			addProperty(jsonString, "Key-Order", gp.getKeyOrder().name(), false);
			addProperty(jsonString, "Key-Uniqueness", gp.isKeyUnique() ? "unique" : "not unique", false);

			jsonString.append("\n\t\t]");
		}

		{
			// output node local properties
			LocalProperties lp = visitable.getLocalProperties();

			jsonString.append(",\n\t\t\"local_properties\": [\n");

			addProperty(jsonString, "Key-Order", lp.getKeyOrder().name(), true);
			addProperty(jsonString, "Key-Uniqueness", lp.isKeyUnique() ? "unique" : "not unique", false);
			addProperty(jsonString, "Key-Grouping", lp.areKeysGrouped() ? "grouped" : "not grouped", false);

			jsonString.append("\n\t\t]");
		}

		// output node size estimates
		jsonString.append(",\n\t\t\"properties\": [\n");

		addProperty(jsonString, "Est. Cardinality", visitable.getEstimatedNumRecords() == -1 ? "(unknown)"
			: formatNumber(visitable.getEstimatedNumRecords()), true);
		addProperty(jsonString, "Est. Key-Cardinality", visitable.getEstimatedKeyCardinality() == -1 ? "(unknown)"
			: formatNumber(visitable.getEstimatedKeyCardinality()), false);
		addProperty(jsonString, "Est. Output Size", visitable.getEstimatedOutputSize() == -1 ? "(unknown)"
			: formatNumber(visitable.getEstimatedOutputSize(), "B"), false);

		jsonString.append("\t\t]");

		// output node cost
		if (visitable.getNodeCosts() != null) {
			jsonString.append(",\n\t\t\"costs\": [\n");

			addProperty(jsonString, "Network", visitable.getNodeCosts().getNetworkCost() == -1 ? "(unknown)"
				: formatNumber(visitable.getNodeCosts().getNetworkCost(), "B"), true);
			addProperty(jsonString, "Disk I/O", visitable.getNodeCosts().getSecondaryStorageCost() == -1 ? "(unknown)"
				: formatNumber(visitable.getNodeCosts().getSecondaryStorageCost(), "B"), false);

			addProperty(jsonString, "Cumulative Network",
				visitable.getCumulativeCosts().getNetworkCost() == -1 ? "(unknown)" : formatNumber(visitable
					.getCumulativeCosts().getNetworkCost(), "B"), false);
			addProperty(jsonString, "Cumulative Disk I/O",
				visitable.getCumulativeCosts().getSecondaryStorageCost() == -1 ? "(unknown)" : formatNumber(visitable
					.getCumulativeCosts().getSecondaryStorageCost(), "B"), false);

			jsonString.append("\n\t\t]");
		}

		// output the node compiler hints
		if (visitable.getPactContract().getCompilerHints() != null) {
			CompilerHints hints = visitable.getPactContract().getCompilerHints();
			CompilerHints defaults = new CompilerHints();

			jsonString.append(",\n\t\t\"compiler_hints\": [\n");

			addProperty(jsonString, "Key-Cardinality",
				hints.getKeyCardinality() == defaults.getKeyCardinality() ? "(none)" : formatNumber(hints
					.getKeyCardinality()), true);
			addProperty(jsonString, "Avg. Records/StubCall", String.valueOf(hints.getAvgRecordsEmittedPerStubCall()), false);
			addProperty(jsonString, "Avg. Values/Key", hints.getAvgNumValuesPerKey() == defaults
				.getAvgNumValuesPerKey() ? "(none)" : String.valueOf(hints.getAvgNumValuesPerKey()), false);
			addProperty(jsonString, "Avg. Width (bytes)", hints.getAvgBytesPerRecord() == defaults
				.getAvgBytesPerRecord() ? "(none)" : String.valueOf(hints.getAvgBytesPerRecord()), false);

			jsonString.append("\t\t]");
		}

		// finish node
		jsonString.append("\n\t},\n");
	}

	private void addProperty(StringBuffer jsonString, String name, String value, boolean first) {
		if (!first) {
			jsonString.append(",\n");
		}
		jsonString.append("\t\t\t{ \"name\": \"");
		jsonString.append(name);
		jsonString.append("\", \"value\": \"");
		jsonString.append(value);
		jsonString.append("\" }");
	}

	public static final String formatNumber(long number) {
		return formatNumber(number, "");
	}

	public static final String formatNumber(long number, String suffix) {
		final int fractionalDigits = 2;

		StringBuilder bld = new StringBuilder();
		bld.append(number);

		int len = bld.length();

		// get the power of 10 / 3
		int pot = (len - (bld.charAt(0) == '-' ? 2 : 1)) / 3;
		if (pot >= SIZE_SUFFIXES.length) {
			pot = SIZE_SUFFIXES.length - 1;
		} else if (pot < 0) {
			pot = 0;
		}

		int beforeDecimal = len - pot * 3;
		if (len > beforeDecimal + fractionalDigits) {
			bld.setLength(beforeDecimal + fractionalDigits);
		}

		// insert decimal point
		if (pot > 0) {
			bld.insert(beforeDecimal, '.');
		}

		// insert number grouping before decimal point
		for (int pos = beforeDecimal - 3; pos > 0; pos -= 3) {
			bld.insert(pos, ',');
		}

		// append the suffix
		bld.append(' ');
		if (pot > 0) {
			bld.append(SIZE_SUFFIXES[pot]);
		}
		bld.append(suffix);

		return bld.toString();
	}

	private static final char[] SIZE_SUFFIXES = { 0, 'K', 'M', 'G', 'T' };
}
