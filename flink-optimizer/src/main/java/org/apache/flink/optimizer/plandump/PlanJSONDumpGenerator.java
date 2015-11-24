/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.optimizer.plandump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.api.common.operators.CompilerHints;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.BinaryUnionNode;
import org.apache.flink.optimizer.dag.BulkIterationNode;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.DataSourceNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.dag.WorksetIterationNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.StringUtils;


public class PlanJSONDumpGenerator {
	
	private Map<DumpableNode<?>, Integer> nodeIds; // resolves pact nodes to ids

	private int nodeCnt;
	
	private boolean encodeForHTML;

	// --------------------------------------------------------------------------------------------
	
	public void setEncodeForHTML(boolean encodeForHTML) {
		this.encodeForHTML = encodeForHTML;
	}
	
	public boolean isEncodeForHTML() {
		return encodeForHTML;
	}
	
	
	public void dumpPactPlanAsJSON(List<DataSinkNode> nodes, PrintWriter writer) {
		@SuppressWarnings("unchecked")
		List<DumpableNode<?>> n = (List<DumpableNode<?>>) (List<?>) nodes;
		compilePlanToJSON(n, writer);
	}
	
	public String getPactPlanAsJSON(List<DataSinkNode> nodes) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		dumpPactPlanAsJSON(nodes, pw);
		return sw.toString();
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
	
	public String getOptimizerPlanAsJSON(OptimizedPlan plan) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		dumpOptimizerPlanAsJSON(plan, pw);
		pw.close();
		return sw.toString();
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
		writer.println("\n\t]\n}");
	}

	private boolean visit(DumpableNode<?> node, PrintWriter writer, boolean first) {
		// check for duplicate traversal
		if (this.nodeIds.containsKey(node)) {
			return false;
		}
		
		// assign an id first
		this.nodeIds.put(node, this.nodeCnt++);
		
		// then recurse
		for (DumpableNode<?> child : node.getPredecessors()) {
			//This is important, because when the node was already in the graph it is not allowed
			//to set first to false!
			if (visit(child, writer, first)) {
				first = false;
			};
		}
		
		// check if this node should be skipped from the dump
		final OptimizerNode n = node.getOptimizerNode();
		
		// ------------------ dump after the ascend ---------------------
		// start a new node and output node id
		if (!first) {
			writer.print(",\n");	
		}
		// open the node
		writer.print("\t{\n");
		
		// recurse, it is is an iteration node
		if (node instanceof BulkIterationNode || node instanceof BulkIterationPlanNode) {
			
			DumpableNode<?> innerChild = node instanceof BulkIterationNode ?
					((BulkIterationNode) node).getNextPartialSolution() :
					((BulkIterationPlanNode) node).getRootOfStepFunction();
					
			DumpableNode<?> begin = node instanceof BulkIterationNode ?
				((BulkIterationNode) node).getPartialSolution() :
				((BulkIterationPlanNode) node).getPartialSolutionPlanNode();
			
			writer.print("\t\t\"step_function\": [\n");
			
			visit(innerChild, writer, true);
			
			writer.print("\n\t\t],\n");
			writer.print("\t\t\"partial_solution\": " + this.nodeIds.get(begin) + ",\n");
			writer.print("\t\t\"next_partial_solution\": " + this.nodeIds.get(innerChild) + ",\n");
		} else if (node instanceof WorksetIterationNode || node instanceof WorksetIterationPlanNode) {
			
			DumpableNode<?> worksetRoot = node instanceof WorksetIterationNode ?
					((WorksetIterationNode) node).getNextWorkset() :
					((WorksetIterationPlanNode) node).getNextWorkSetPlanNode();
			DumpableNode<?> solutionDelta = node instanceof WorksetIterationNode ?
					((WorksetIterationNode) node).getSolutionSetDelta() :
					((WorksetIterationPlanNode) node).getSolutionSetDeltaPlanNode();
					
			DumpableNode<?> workset = node instanceof WorksetIterationNode ?
						((WorksetIterationNode) node).getWorksetNode() :
						((WorksetIterationPlanNode) node).getWorksetPlanNode();
			DumpableNode<?> solutionSet = node instanceof WorksetIterationNode ?
						((WorksetIterationNode) node).getSolutionSetNode() :
						((WorksetIterationPlanNode) node).getSolutionSetPlanNode();
			
			writer.print("\t\t\"step_function\": [\n");
			
			visit(worksetRoot, writer, true);
			visit(solutionDelta, writer, false);
			
			writer.print("\n\t\t],\n");
			writer.print("\t\t\"workset\": " + this.nodeIds.get(workset) + ",\n");
			writer.print("\t\t\"solution_set\": " + this.nodeIds.get(solutionSet) + ",\n");
			writer.print("\t\t\"next_workset\": " + this.nodeIds.get(worksetRoot) + ",\n");
			writer.print("\t\t\"solution_delta\": " + this.nodeIds.get(solutionDelta) + ",\n");
		}
		
		// print the id
		writer.print("\t\t\"id\": " + this.nodeIds.get(node));

		
		final String type;
		String contents;
		if (n instanceof DataSinkNode) {
			type = "sink";
			contents = n.getOperator().toString();
		} else if (n instanceof DataSourceNode) {
			type = "source";
			contents = n.getOperator().toString();
		}
		else if (n instanceof BulkIterationNode) {
			type = "bulk_iteration";
			contents = n.getOperator().getName();
		}
		else if (n instanceof WorksetIterationNode) {
			type = "workset_iteration";
			contents = n.getOperator().getName();
		}
		else if (n instanceof BinaryUnionNode) {
			type = "pact";
			contents = "";
		}
		else {
			type = "pact";
			contents = n.getOperator().getName();
		}
		
		contents = StringUtils.showControlCharacters(contents);
		if (encodeForHTML) {
			contents = StringEscapeUtils.escapeHtml4(contents);
			contents = contents.replace("\\", "&#92;");
		}
		
		
		String name = n.getOperatorName();
		if (name.equals("Reduce") && (node instanceof SingleInputPlanNode) && 
				((SingleInputPlanNode) node).getDriverStrategy() == DriverStrategy.SORTED_GROUP_COMBINE) {
			name = "Combine";
		}
		
		// output the type identifier
		writer.print(",\n\t\t\"type\": \"" + type + "\"");
		
		// output node name
		writer.print(",\n\t\t\"pact\": \"" + name + "\"");
		
		// output node contents
		writer.print(",\n\t\t\"contents\": \"" + contents + "\"");

		// parallelism
		writer.print(",\n\t\t\"parallelism\": \""
			+ (n.getParallelism() >= 1 ? n.getParallelism() : "default") + "\"");
		
		// output node predecessors
		Iterator<? extends DumpableConnection<?>> inConns = node.getDumpableInputs().iterator();
		String child1name = "", child2name = "";

		if (inConns != null && inConns.hasNext()) {
			// start predecessor list
			writer.print(",\n\t\t\"predecessors\": [");
			int inputNum = 0;
			
			while (inConns.hasNext()) {
				final DumpableConnection<?> inConn = inConns.next();
				final DumpableNode<?> source = inConn.getSource();
				writer.print(inputNum == 0 ? "\n" : ",\n");
				if (inputNum == 0) {
					child1name += child1name.length() > 0 ? ", " : ""; 
					child1name += source.getOptimizerNode().getOperator().getName();
				} else if (inputNum == 1) {
					child2name += child2name.length() > 0 ? ", " : ""; 
					child2name = source.getOptimizerNode().getOperator().getName();
				}

				// output predecessor id
				writer.print("\t\t\t{\"id\": " + this.nodeIds.get(source));

				// output connection side
				if (inConns.hasNext() || inputNum > 0) {
					writer.print(", \"side\": \"" + (inputNum == 0 ? "first" : "second") + "\"");
				}
				// output shipping strategy and channel type
				final Channel channel = (inConn instanceof Channel) ? (Channel) inConn : null; 
				final ShipStrategyType shipType = channel != null ? 
						channel.getShipStrategy() :
						inConn.getShipStrategy();
					
				String shipStrategy = null;
				if (shipType != null) {
					switch (shipType) {
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
					case PARTITION_RANDOM:
						shipStrategy = "Redistribute";
						break;
					case PARTITION_FORCED_REBALANCE:
						shipStrategy = "Rebalance";
						break;
					case PARTITION_CUSTOM:
						shipStrategy = "Custom Partition";
						break;
					default:
						throw new CompilerException("Unknown ship strategy '" + inConn.getShipStrategy().name()
							+ "' in JSON generator.");
					}
				}
				
				if (channel != null && channel.getShipStrategyKeys() != null && channel.getShipStrategyKeys().size() > 0) {
					shipStrategy += " on " + (channel.getShipStrategySortOrder() == null ?
							channel.getShipStrategyKeys().toString() :
							Utils.createOrdering(channel.getShipStrategyKeys(), channel.getShipStrategySortOrder()).toString());
				}

				if (shipStrategy != null) {
					writer.print(", \"ship_strategy\": \"" + shipStrategy + "\"");
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
					
					if (channel != null && channel.getLocalStrategyKeys() != null && channel.getLocalStrategyKeys().size() > 0) {
						localStrategy += " on " + (channel.getLocalStrategySortOrder() == null ?
								channel.getLocalStrategyKeys().toString() :
								Utils.createOrdering(channel.getLocalStrategyKeys(), channel.getLocalStrategySortOrder()).toString());
					}
					
					if (localStrategy != null) {
						writer.print(", \"local_strategy\": \"" + localStrategy + "\"");
					}
					
					if (channel != null && channel.getTempMode() != TempMode.NONE) {
						String tempMode = channel.getTempMode().toString();
						writer.print(", \"temp_mode\": \"" + tempMode + "\"");
					}

					if (channel != null) {
						String exchangeMode = channel.getDataExchangeMode().toString();
						writer.print(", \"exchange_mode\": \"" + exchangeMode + "\"");
					}
				}
				
				writer.print('}');
				inputNum++;
			}
			// finish predecessors
			writer.print("\n\t\t]");
		}
		
		//---------------------------------------------------------------------------------------
		// the part below here is relevant only to plan nodes with concrete strategies, etc
		//---------------------------------------------------------------------------------------

		final PlanNode p = node.getPlanNode();
		if (p == null) {
			// finish node
			writer.print("\n\t}");
			return true;
		}
		// local strategy
		String locString = null;
		if (p.getDriverStrategy() != null) {
			switch (p.getDriverStrategy()) {
			case NONE:
			case BINARY_NO_OP:
				break;
				
			case UNARY_NO_OP:
				locString = "No-Op";
				break;
				
			case COLLECTOR_MAP:
			case MAP:
				locString = "Map";
				break;
				
			case FLAT_MAP:
				locString = "FlatMap";
				break;
				
			case MAP_PARTITION:
				locString = "Map Partition";
				break;
			
			case ALL_REDUCE:
				locString = "Reduce All";
				break;
			
			case ALL_GROUP_REDUCE:
			case ALL_GROUP_REDUCE_COMBINE:
				locString = "Group Reduce All";
				break;
				
			case SORTED_REDUCE:
				locString = "Sorted Reduce";
				break;
				
			case SORTED_PARTIAL_REDUCE:
				locString = "Sorted Combine/Reduce";
				break;

			case SORTED_GROUP_REDUCE:
				locString = "Sorted Group Reduce";
				break;
				
			case SORTED_GROUP_COMBINE:
				locString = "Sorted Combine";
				break;

			case HYBRIDHASH_BUILD_FIRST:
				locString = "Hybrid Hash (build: " + child1name + ")";
				break;
			case HYBRIDHASH_BUILD_SECOND:
				locString = "Hybrid Hash (build: " + child2name + ")";
				break;
				
			case HYBRIDHASH_BUILD_FIRST_CACHED:
				locString = "Hybrid Hash (CACHED) (build: " + child1name + ")";
				break;
			case HYBRIDHASH_BUILD_SECOND_CACHED:
				locString = "Hybrid Hash (CACHED) (build: " + child2name + ")";
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

			case INNER_MERGE:
				locString = "Merge";
				break;

			case CO_GROUP:
				locString = "Co-Group";
				break;

			default:
				locString = p.getDriverStrategy().name();
				break;
			}

			if (locString != null) {
				writer.print(",\n\t\t\"driver_strategy\": \"");
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
		writer.print(",\n\t\t\"estimates\": [\n");

		addProperty(writer, "Est. Output Size", n.getEstimatedOutputSize() == -1 ? "(unknown)"
			: formatNumber(n.getEstimatedOutputSize(), "B"), true);
		addProperty(writer, "Est. Cardinality", n.getEstimatedNumRecords() == -1 ? "(unknown)"
			: formatNumber(n.getEstimatedNumRecords()), false);

		writer.print("\t\t]");

		// output node cost
		if (p.getNodeCosts() != null) {
			writer.print(",\n\t\t\"costs\": [\n");

			addProperty(writer, "Network", p.getNodeCosts().getNetworkCost() == -1 ? "(unknown)"
				: formatNumber(p.getNodeCosts().getNetworkCost(), "B"), true);
			addProperty(writer, "Disk I/O", p.getNodeCosts().getDiskCost() == -1 ? "(unknown)"
				: formatNumber(p.getNodeCosts().getDiskCost(), "B"), false);
			addProperty(writer, "CPU", p.getNodeCosts().getCpuCost() == -1 ? "(unknown)"
				: formatNumber(p.getNodeCosts().getCpuCost(), ""), false);

			addProperty(writer, "Cumulative Network",
				p.getCumulativeCosts().getNetworkCost() == -1 ? "(unknown)" : formatNumber(p
					.getCumulativeCosts().getNetworkCost(), "B"), false);
			addProperty(writer, "Cumulative Disk I/O",
				p.getCumulativeCosts().getDiskCost() == -1 ? "(unknown)" : formatNumber(p
					.getCumulativeCosts().getDiskCost(), "B"), false);
			addProperty(writer, "Cumulative CPU",
				p.getCumulativeCosts().getCpuCost() == -1 ? "(unknown)" : formatNumber(p
					.getCumulativeCosts().getCpuCost(), ""), false);

			writer.print("\n\t\t]");
		}

		// output the node compiler hints
		if (n.getOperator().getCompilerHints() != null) {
			CompilerHints hints = n.getOperator().getCompilerHints();
			CompilerHints defaults = new CompilerHints();

			String size = hints.getOutputSize() == defaults.getOutputSize() ? "(none)" : String.valueOf(hints.getOutputSize());
			String card = hints.getOutputCardinality() == defaults.getOutputCardinality() ? "(none)" : String.valueOf(hints.getOutputCardinality());
			String width = hints.getAvgOutputRecordSize() == defaults.getAvgOutputRecordSize() ? "(none)" : String.valueOf(hints.getAvgOutputRecordSize());
			String filter = hints.getFilterFactor() == defaults.getFilterFactor() ? "(none)" : String.valueOf(hints.getFilterFactor());
			
			writer.print(",\n\t\t\"compiler_hints\": [\n");

			addProperty(writer, "Output Size (bytes)", size, true);
			addProperty(writer, "Output Cardinality", card, false);
			addProperty(writer, "Avg. Output Record Size (bytes)", width, false);
			addProperty(writer, "Filter Factor", filter, false);

			writer.print("\t\t]");
		}

		// finish node
		writer.print("\n\t}");
		return true;
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

	public static String formatNumber(double number) {
		return formatNumber(number, "");
	}

	public static String formatNumber(double number, String suffix) {
		if (number <= 0.0) {
			return String.valueOf(number);
		}

		int power = (int) Math.ceil(Math.log10(number));

		int group = (power - 1) / 3;
		if (group >= SIZE_SUFFIXES.length) {
			group = SIZE_SUFFIXES.length - 1;
		} else if (group < 0) {
			group = 0;
		}

		// truncate fractional part
		int beforeDecimal = power - group * 3;
		if (power > beforeDecimal) {
			for (int i = power - beforeDecimal; i > 0; i--) {
				number /= 10;
			}
		}
		
		return group > 0 ? String.format(Locale.US, "%.2f %s", number, SIZE_SUFFIXES[group]) :
			String.format(Locale.US, "%.2f", number);
	}

	private static final char[] SIZE_SUFFIXES = { 0, 'K', 'M', 'G', 'T' };
}
