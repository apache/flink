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

package eu.stratosphere.pact.compiler.plandump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.TempMode;
import eu.stratosphere.pact.compiler.plan.candidate.UnionPlanNode;
import eu.stratosphere.pact.compiler.util.Utils;

/**
 * 
 */
public class PlanJSONDumpGenerator
{
	private Map<DumpableNode<?>, Integer> nodeIds; // resolves pact nodes to ids

	private int nodeCnt; // resolves pact nodes to ids

	// --------------------------------------------------------------------------------------------
	
	public void dumpPactPlanAsJSON(List<DataSinkNode> nodes, PrintWriter writer) {
		@SuppressWarnings("unchecked")
		List<DumpableNode<?>> n = (List<DumpableNode<?>>) (List<?>) nodes;
		compilePlanToJSON(n, writer);
	}
	
	public void dumpOptimizerPlanAsJSON(OptimizedPlan plan, File toFile) throws IOException {
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(toFile), false);
			dumpOptimizerPlanAsJSON(plan, pw);
			pw.flush();
		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}
	
	public void dumpOptimizerPlanAsJSON(OptimizedPlan plan, PrintWriter writer) {
		Collection<SinkPlanNode> sinks = plan.getDataSinks();
		if (sinks instanceof List) {
			dumpOptimizerPlanAsJSON((List<SinkPlanNode>) sinks, writer);
		} else {
			List<SinkPlanNode> n = new ArrayList<SinkPlanNode>();
			n.addAll(sinks);
			dumpOptimizerPlanAsJSON(n, writer);
		}
	}
	
	public void dumpOptimizerPlanAsJSON(List<SinkPlanNode> nodes, PrintWriter writer) {
		@SuppressWarnings("unchecked")
		List<DumpableNode<?>> n = (List<DumpableNode<?>>) (List<?>) nodes;
		compilePlanToJSON(n, writer);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private void compilePlanToJSON(List<DumpableNode<?>> nodes, PrintWriter writer) {
		// initialization to assign node ids
		this.nodeIds = new HashMap<DumpableNode<?>, Integer>();
		this.nodeCnt = 0;
		
		// JSON header
		writer.print("{\n\t\"nodes\": [\n\n");

		// Generate JSON for plan
		for (int i = 0; i < nodes.size(); i++) {
			visit(nodes.get(i), writer, i == 0);
		}
		
		// JSON Footer
		writer.println("\t]\n}");
	}

	private void visit(DumpableNode<?> node, PrintWriter writer, boolean first) {
		// check for duplicate traversal
		if (this.nodeIds.containsKey(node)) {
			return;
		}
		
		// assign an id first
		this.nodeIds.put(node, this.nodeCnt++);
		
		// then recurse
		for (Iterator<? extends DumpableNode<?>> children = node.getPredecessors(); children.hasNext(); ) {
			final DumpableNode<?> child = children.next();
			visit(child, writer, false);
		}
		
		// check if this node should be skipped from the dump
		final OptimizerNode n = node.getOptimizerNode();
		
		// ------------------ dump after the ascend ---------------------
		// start a new node and output node id
		writer.print("\t{\n\t\t\"id\": " + this.nodeIds.get(node));
		
		// output the type identifier
		final String type;
		switch (n.getPactType()) {
		case DataSink:
			type = "sink";
			break;
		case DataSource:
			type = "source";
			break;
		case Union:
			type = "pact";
			break;
		default:
			type = "pact";
			break;
		}
		writer.print(",\n\t\t\"type\": \"" + type + "\"");
		
		// output node name
		writer.print(",\n\t\t\"pact\": \"" + n.getName() + "\"");
		
		// output node contents
		final String contents;
		switch (n.getPactType()) {
		case DataSink:
			contents = n.getPactContract().toString();
			break;
		case DataSource:
			contents = n.getPactContract().toString();
			break;
		case Union:
			contents = "";
			break;
		default:
			contents = n.getPactContract().getName();
			break;
		}
		
		writer.print(",\n\t\t\"contents\": \"" + contents + "\"");

		// degree of parallelism
		writer.print(",\n\t\t\"parallelism\": \""
			+ (n.getDegreeOfParallelism() >= 1 ? n.getDegreeOfParallelism() : "default") + "\"");

		// output node predecessors
		Iterator<? extends DumpableConnection<?>> inConns = node.getDumpableInputs();
		String child1name = "", child2name = "";

		if (inConns != null && inConns.hasNext()) {
			// start predecessor list
			writer.print(",\n\t\t\"predecessors\": [");
			int connNum = 0;
			int inputNum = 0;
			
			while (inConns.hasNext()) {
				final DumpableConnection<?> conn = inConns.next();
				
				final Collection<DumpableConnection<?>> inConnsForInput;
				if (conn.getSource() instanceof UnionPlanNode) {
					inConnsForInput = new ArrayList<DumpableConnection<?>>();
					
					for (Iterator<? extends DumpableConnection<?>> inputOfUnion = conn.getSource().getDumpableInputs(); inputOfUnion.hasNext();) {
						inConnsForInput.add(inputOfUnion.next());
					}
				}
				else {
					inConnsForInput = Collections.<DumpableConnection<?>>singleton(conn);
				}
				
				for (DumpableConnection<?> inConn : inConnsForInput) {
					final DumpableNode<?> source = inConn.getSource();
					writer.print(connNum == 0 ? "\n" : ",\n");
					if (connNum == 0) {
						child1name += child1name.length() > 0 ? ", " : ""; 
						child1name += source.getOptimizerNode().getPactContract().getName();
					} else if (connNum == 1) {
						child2name += child2name.length() > 0 ? ", " : ""; 
						child2name = source.getOptimizerNode().getPactContract().getName();
					}
	
					// output predecessor id
					writer.print("\t\t\t{\"id\": " + this.nodeIds.get(source));
	
					// output connection side
					if (inConns.hasNext()) {
						writer.print(", \"side\": \"" + (inputNum == 0 ? "first" : "second") + "\"");
					}
					// output shipping strategy and channel type
					final Channel channel = (inConn instanceof Channel) ? (Channel) inConn : null; 
					String shipStrategy = null;
					switch (conn.getShipStrategy()) {
					case NONE:
						// nothing
						break;
					case FORWARD:
						shipStrategy = "Forward";
						break;
					case BROADCAST:
						shipStrategy = "Broadcast";
						break;
					case PARTITION_HASH:
						shipStrategy = "Hash Partition";
						break;
					case PARTITION_RANGE:
						shipStrategy = "Range Partition";
						break;
					case PARTITION_LOCAL_HASH:
						shipStrategy = "Hash Partition (local)";
						break;
					default:
						throw new CompilerException("Unknown ship strategy '" + conn.getShipStrategy().name()
							+ "' in JSON generator.");
					}
					
					if (channel != null && channel.getShipStrategyKeys() != null && channel.getShipStrategyKeys().size() > 0) {
						shipStrategy += " on " + (channel.getShipStrategySortOrder() == null ?
								channel.getShipStrategyKeys().toString() :
								Utils.createOrdering(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder()).toString());
					}
	
					if (shipStrategy != null) {
						writer.print(", \"shippingStrategy\": \"" + shipStrategy + "\"");
					}
					
					if (channel != null) {
						String localStrategy = null;
						switch (channel.getLocalStrategy()) {
						case NONE:
							break;
						case SORT:
							localStrategy = "Sort";
							break;
						case COMBININGSORT:
							localStrategy = "Sort (combining)";
							break;
						default:
							throw new CompilerException("Unknown local strategy " + channel.getLocalStrategy().name());
						}
						
						if (channel.getLocalStrategyKeys() != null && channel.getLocalStrategyKeys().size() > 0) {
							localStrategy += " on " + (channel.getLocalStrategySortOrder() == null ?
									channel.getLocalStrategyKeys().toString() :
									Utils.createOrdering(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder()).toString());
						}
						
						if (localStrategy != null) {
							writer.print(", \"localStrategy\": \"" + localStrategy + "\"");
						}
						
						if (channel.getTempMode() != TempMode.NONE) {
							String tempMode = channel.getTempMode().toString();
							writer.print(", \"tempMode\": \"" + tempMode + "\"");
						}
					}
					
					writer.print('}');
					connNum++;
				}
				inputNum++;
			}
			// finish predecessors
			writer.print("\t\t]");
		}
		
		//---------------------------------------------------------------------------------------
		// the part below here is relevant only to plan nodes with concrete strategies, etc
		//---------------------------------------------------------------------------------------

		final PlanNode p = node.getPlanNode();
		if (p == null) {
			return ;
		}
		
		// local strategy
		String locString = null;
		if (p.getDriverStrategy() != null) {
			switch (p.getDriverStrategy()) {
			case NONE:
				break;
			case MAP:
				locString = "Map";
				break;
			case PARTIAL_GROUP:
				locString = "Ordered Partial Grouping";
				break;
			case GROUP_OVER_ORDERED:
				locString = "Ordered Grouping";
				break;
			case HYBRIDHASH_BUILD_FIRST:
				locString = "Hybrid Hash (build: " + child1name + ")";
				break;
			case HYBRIDHASH_BUILD_SECOND:
				locString = "Hybrid Hash (build: " + child2name + ")";
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
			case MERGE:
				locString = "Merge";
				break;
			case CO_GROUP:
				locString = "Co-Group";
				break;
			default:
				throw new CompilerException("Unknown local strategy '" + p.getDriverStrategy().name()
					+ "' in JSON generator.");
			}

			if (locString != null) {
				writer.print(",\n\t\t\"local_strategy\": \"");
				writer.print(locString);
				writer.print("\"");
			}
		}
		
		{
			// output node global properties
			final GlobalProperties gp = p.getGlobalProperties();

			writer.print(",\n\t\t\"global_properties\": [\n");

			addProperty(writer, "Partitioning", gp.getPartitioning().name(), true);
			if (gp.getPartitioningFields() != null) {
				addProperty(writer, "Partitioned on", gp.getPartitioningFields().toString(), false);
			}
			if (gp.getPartitioningOrdering() != null) {
				addProperty(writer, "Partitioning Order", gp.getPartitioningOrdering().toString(), false);	
			}
			else {
				addProperty(writer, "Partitioning Order", "(none)", false);
			}
			if (n.getUniqueFields() == null || n.getUniqueFields().size() == 0) {
				addProperty(writer, "Uniqueness", "not unique", false);
			}
			else {
				addProperty(writer, "Uniqueness", n.getUniqueFields().toString(), false);	
			}

			writer.print("\n\t\t]");
		}

		{
			// output node local properties
			LocalProperties lp = p.getLocalProperties();

			writer.print(",\n\t\t\"local_properties\": [\n");

			if (lp.getOrdering() != null) {
				addProperty(writer, "Order", lp.getOrdering().toString(), true);	
			}
			else {
				addProperty(writer, "Order", "(none)", true);
			}
			if (lp.getGroupedFields() != null && lp.getGroupedFields().size() > 0) {
				addProperty(writer, "Grouped on", lp.getGroupedFields().toString(), false);
			} else {
				addProperty(writer, "Grouping", "not grouped", false);	
			}
			if (n.getUniqueFields() == null || n.getUniqueFields().size() == 0) {
				addProperty(writer, "Uniqueness", "not unique", false);
			}
			else {
				addProperty(writer, "Uniqueness", n.getUniqueFields().toString(), false);	
			}

			writer.print("\n\t\t]");
		}

		// output node size estimates
		writer.print(",\n\t\t\"properties\": [\n");

		addProperty(writer, "Est. Cardinality", n.getEstimatedNumRecords() == -1 ? "(unknown)"
			: formatNumber(n.getEstimatedNumRecords()), true);
		String estCardinality = "(unknown)";
		if (n.getEstimatedCardinalities().size() > 0) {
			estCardinality = "";
			for (Entry<FieldSet, Long> entry : n.getEstimatedCardinalities().entrySet()) {
				estCardinality += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
			}
		}
		addProperty(writer, "Est. Cardinality/fields", estCardinality, false);
		addProperty(writer, "Est. Output Size", n.getEstimatedOutputSize() == -1 ? "(unknown)"
			: formatNumber(n.getEstimatedOutputSize(), "B"), false);

		writer.print("\t\t]");

		// output node cost
		if (p.getNodeCosts() != null) {
			writer.print(",\n\t\t\"costs\": [\n");

			addProperty(writer, "Network", p.getNodeCosts().getNetworkCost() == -1 ? "(unknown)"
				: formatNumber(p.getNodeCosts().getNetworkCost(), "B"), true);
			addProperty(writer, "Disk I/O", p.getNodeCosts().getSecondaryStorageCost() == -1 ? "(unknown)"
				: formatNumber(p.getNodeCosts().getSecondaryStorageCost(), "B"), false);

			addProperty(writer, "Cumulative Network",
				p.getCumulativeCosts().getNetworkCost() == -1 ? "(unknown)" : formatNumber(p
					.getCumulativeCosts().getNetworkCost(), "B"), false);
			addProperty(writer, "Cumulative Disk I/O",
				p.getCumulativeCosts().getSecondaryStorageCost() == -1 ? "(unknown)" : formatNumber(p
					.getCumulativeCosts().getSecondaryStorageCost(), "B"), false);

			writer.print("\n\t\t]");
		}

		// output the node compiler hints
		if (n.getPactContract().getCompilerHints() != null) {
			CompilerHints hints = n.getPactContract().getCompilerHints();
			CompilerHints defaults = new CompilerHints();

			writer.print(",\n\t\t\"compiler_hints\": [\n");

			String hintCardinality;
			if (hints.getDistinctCounts().size() > 0) {
				hintCardinality = "";
				for (Entry<FieldSet, Long> entry : hints.getDistinctCounts().entrySet()) {
					hintCardinality += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
				}
			} else {
				hintCardinality = "(none)";
			}
			addProperty(writer, "Cardinality", hintCardinality, true);
			
			addProperty(writer, "Avg. Records/StubCall", hints.getAvgRecordsEmittedPerStubCall() == defaults.
					getAvgRecordsEmittedPerStubCall() ? "(none)" : String.valueOf(hints.getAvgRecordsEmittedPerStubCall()), false);
			
			String valuesKey;
			if (hints.getAvgNumRecordsPerDistinctFields().size() > 0) {
				valuesKey = "";
				for (Entry<FieldSet, Float> entry : hints.getAvgNumRecordsPerDistinctFields().entrySet()) {
					valuesKey += "[" + entry.getKey().toString() + "->" + entry.getValue() + "]"; 
				}
			} else {
				valuesKey = "(none)";
			}
			addProperty(writer, "Avg. Values/Distinct fields", valuesKey, false);
			
			addProperty(writer, "Avg. Width (bytes)", hints.getAvgBytesPerRecord() == defaults
				.getAvgBytesPerRecord() ? "(none)" : String.valueOf(hints.getAvgBytesPerRecord()), false);

			writer.print("\t\t]");
		}

		// finish node
		writer.print("\n\t},\n");
	}

	private void addProperty(PrintWriter writer, String name, String value, boolean first) {
		if (!first) {
			writer.print(",\n");
		}
		writer.print("\t\t\t{ \"name\": \"");
		writer.print(name);
		writer.print("\", \"value\": \"");
		writer.print(value);
		writer.print("\" }");
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
