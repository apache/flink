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
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.PactConnection.TempMode;
import eu.stratosphere.pact.compiler.plan.UnionNode;

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
		this.jsonString.deleteCharAt(this.jsonString.lastIndexOf(","));
		this.jsonString.append("\t]\n}");

		// return JSON string
		return this.jsonString.toString();
	}

	@Override
	public boolean preVisit(OptimizerNode visitable) {

		if (visitable instanceof UnionNode) return true;
		
		// visit each node and assign a unique id
		if (!this.nodeIds.containsKey(visitable)) {
			this.nodeIds.put(visitable, this.nodeCnt++);
			return true;
		}
		
		return false;
	}

	@Override
	public void postVisit(OptimizerNode visitable) {

		// determine node type
		String type;
		switch (visitable.getPactType()) {
		case DataSink:
			type = "sink";
			break;
		case DataSource:
			type = "source";
			break;
		case Union:
			return;
		default:
			type = "pact";
			break;
		}
		
		// start a new node
		this.jsonString.append("\t{\n");

		// output node id
		this.jsonString.append("\t\t\"id\": " + this.nodeIds.get(visitable));
		
		this.jsonString.append(",\n\t\t\"type\": \"" + type + "\"");

		// output node contents
		String contents;
		switch (visitable.getPactType()) {
		case DataSink:
			contents = visitable.getPactContract().toString();
			break;
		case DataSource:
			contents = visitable.getPactContract().toString();
			break;
		default:
			jsonString.append(",\n\t\t\"pact\": \"" + visitable.getName() + "\"");
			contents = visitable.getPactContract().getName();
			break;
		}
		this.jsonString.append(",\n\t\t\"contents\": \"" + contents + "\"");

		// output contract
//		OutputContract outContr = visitable.getOutputContract();
//		if (outContr != null && outContr != OutputContract.None) {
//			jsonString.append(",\n\t\t\"outputcontract\": \"" + outContr.name() + "\"");
//		}

		// degree of parallelism
		this.jsonString.append(",\n\t\t\"parallelism\": \""
			+ (visitable.getDegreeOfParallelism() >= 1 ? visitable.getDegreeOfParallelism() : "default") + "\"");

		// output node predecessors
		List<PactConnection> inConns = visitable.getIncomingConnections();
		String child1name = "", child2name = "";

		if (inConns != null && inConns.size() > 0) {
			// start predecessor list
			this.jsonString.append(",\n\t\t\"predecessors\": [");
			int connCnt = 0;
			int inputNum = 0;
			for(PactConnection conn : inConns) {
				
				
				OptimizerNode inputNode = conn.getSourcePact();
				List<OptimizerNode> inConnForInput;
				
				if (inputNode instanceof UnionNode) {
					inConnForInput = new LinkedList<OptimizerNode>();
					for (PactConnection inputOfUnion : inputNode.getIncomingConnections()) {
						inConnForInput.add(inputOfUnion.getSourcePact());
					}
				}
				else {
					inConnForInput = Collections.singletonList(inputNode);
				}
				
				for (OptimizerNode source : inConnForInput) {
					this.jsonString.append(connCnt == 0 ? "\n" : ",\n");
					if (connCnt == 0) {
						child1name += child1name.length() > 0 ? ", " : ""; 
						child1name += source.getPactContract().getName();
					} else if (connCnt == 1) {
						child2name += child2name.length() > 0 ? ", " : ""; 
						child2name = source.getPactContract().getName();
					}
	
					// output predecessor id
					this.jsonString.append("\t\t\t{\"id\": " + this.nodeIds.get(source));
	
					// output connection side
					if (inConns.size() == 2) {
						this.jsonString.append(", \"side\": \"" + (inputNum == 0 ? "first" : "second") + "\"");
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
						this.jsonString.append(", \"shippingStrategy\": \"" + shipStrategy + "\"");
					}
					if (channelType != null) {
						this.jsonString.append(", \"channelType\": \"" + channelType + "\"");
					}
	
					if (conn.getTempMode() != TempMode.NONE) {
						String tempMode = conn.getTempMode().toString();
						this.jsonString.append(", \"tempMode\": \"" + tempMode + "\"");
					}
	
					this.jsonString.append('}');
					
					connCnt++;
				}
					
				inputNum++;
			}
			// finish predecessors
			this.jsonString.append("\t\t]");
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
				this.jsonString.append(",\n\t\t\"local_strategy\": \"");
				this.jsonString.append(locString);
				this.jsonString.append("\"");
			}
		}

		{
			// output node global properties
			GlobalProperties gp = visitable.getGlobalProperties();

			this.jsonString.append(",\n\t\t\"global_properties\": [\n");

			addProperty(jsonString, "Partitioning", gp.getPartitioning().name(), true);
			if (gp.getPartitioning() != PartitionProperty.NONE) {
				addProperty(jsonString, "Partitioned on", gp.getPartitionedFields().toString(), false);
			}
			if (gp.getOrdering() != null) {
				addProperty(jsonString, "Order", gp.getOrdering().toString(), false);	
			}
			else {
				addProperty(jsonString, "Order", "(none)", false);
			}
			if (visitable.getUniqueFields() == null || visitable.getUniqueFields().size() == 0) {
				addProperty(jsonString, "Uniqueness", "not unique", false);
			}
			else {
				addProperty(jsonString, "Uniqueness", visitable.getUniqueFields().toString(), false);	
			}

			this.jsonString.append("\n\t\t]");
		}

		{
			// output node local properties
			LocalProperties lp = visitable.getLocalProperties();

			this.jsonString.append(",\n\t\t\"local_properties\": [\n");

			if (lp.getOrdering() != null) {
				addProperty(jsonString, "Order", lp.getOrdering().toString(), true);	
			}
			else {
				addProperty(jsonString, "Order", "(none)", true);
			}
			if (visitable.getUniqueFields() == null || visitable.getUniqueFields().size() == 0) {
				addProperty(jsonString, "Uniqueness", "not unique", false);
			}
			else {
				addProperty(jsonString, "Uniqueness", visitable.getUniqueFields().toString(), false);	
			}
			addProperty(jsonString, "Grouping", lp.isGrouped() ? "grouped": "not grouped", false);
			if (lp.isGrouped()) {
				addProperty(jsonString, "Grouped on", lp.getGroupedFields().toString(), false);	
			}

			this.jsonString.append("\n\t\t]");
		}

		// output node size estimates
		this.jsonString.append(",\n\t\t\"properties\": [\n");

		addProperty(this.jsonString, "Est. Cardinality", visitable.getEstimatedNumRecords() == -1 ? "(unknown)"
			: formatNumber(visitable.getEstimatedNumRecords()), true);
		String estCardinality = "(unknown)";
		if (visitable.getEstimatedCardinalities().size() > 0) {
			estCardinality = "";
			for (Entry<FieldSet, Long> entry : visitable.getEstimatedCardinalities().entrySet()) {
				estCardinality += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
			}
		}
		addProperty(jsonString, "Est. Cardinality/fields", estCardinality, false);	
		addProperty(jsonString, "Est. Output Size", visitable.getEstimatedOutputSize() == -1 ? "(unknown)"
			: formatNumber(visitable.getEstimatedOutputSize(), "B"), false);

		this.jsonString.append("\t\t]");

		// output node cost
		if (visitable.getNodeCosts() != null) {
			this.jsonString.append(",\n\t\t\"costs\": [\n");

			addProperty(this.jsonString, "Network", visitable.getNodeCosts().getNetworkCost() == -1 ? "(unknown)"
				: formatNumber(visitable.getNodeCosts().getNetworkCost(), "B"), true);
			addProperty(this.jsonString, "Disk I/O", visitable.getNodeCosts().getSecondaryStorageCost() == -1 ? "(unknown)"
				: formatNumber(visitable.getNodeCosts().getSecondaryStorageCost(), "B"), false);

			addProperty(this.jsonString, "Cumulative Network",
				visitable.getCumulativeCosts().getNetworkCost() == -1 ? "(unknown)" : formatNumber(visitable
					.getCumulativeCosts().getNetworkCost(), "B"), false);
			addProperty(this.jsonString, "Cumulative Disk I/O",
				visitable.getCumulativeCosts().getSecondaryStorageCost() == -1 ? "(unknown)" : formatNumber(visitable
					.getCumulativeCosts().getSecondaryStorageCost(), "B"), false);

			this.jsonString.append("\n\t\t]");
		}

		// output the node compiler hints
		if (visitable.getPactContract().getCompilerHints() != null) {
			CompilerHints hints = visitable.getPactContract().getCompilerHints();
			CompilerHints defaults = new CompilerHints();

			this.jsonString.append(",\n\t\t\"compiler_hints\": [\n");

			String hintCardinality = "(none)";
			if (hints.getDistinctCounts().size() > 0) {
				hintCardinality = "";
				for (Entry<FieldSet, Long> entry : visitable.getEstimatedCardinalities().entrySet()) {
					hintCardinality += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
				}
			}
			addProperty(jsonString, "Cardinality", hintCardinality, true);
			addProperty(jsonString, "Avg. Records/StubCall", hints.getAvgRecordsEmittedPerStubCall() == defaults.
					getAvgRecordsEmittedPerStubCall() ? "(none)" : String.valueOf(hints.getAvgRecordsEmittedPerStubCall()), false);
			
			String valuesKey = "(none)";
			if (hints.getAvgNumRecordsPerDistinctFields().size() > 0) {
				valuesKey = "";
				for (Entry<FieldSet, Float> entry : hints.getAvgNumRecordsPerDistinctFields().entrySet()) {
					valuesKey += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
				}
			}
			addProperty(jsonString, "Avg. Values/Distinct fields", valuesKey, false);
			addProperty(jsonString, "Avg. Width (bytes)", hints.getAvgBytesPerRecord() == defaults
				.getAvgBytesPerRecord() ? "(none)" : String.valueOf(hints.getAvgBytesPerRecord()), false);

			this.jsonString.append("\t\t]");
		}

		// finish node
		this.jsonString.append("\n\t},\n");
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
