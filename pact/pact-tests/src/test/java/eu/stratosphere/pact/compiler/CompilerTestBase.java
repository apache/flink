/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.compiler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;

import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.WorksetIteration;
import eu.stratosphere.pact.generic.io.FileInputFormat.FileBaseStatistics;

/**
 *
 */
public abstract class CompilerTestBase {

	protected static final String IN_FILE = isWindows() ? "file://c:\\" : "file:///dev/random";
	
	protected static final String OUT_FILE = isWindows() ? "file://c:\\" : "file:///dev/null";
	
	protected static final int DEFAULT_PARALLELISM = 8;
	
	protected static final String DEFAULT_PARALLELISM_STRING = String.valueOf(DEFAULT_PARALLELISM);
	
	private static final String CACHE_KEY = "cachekey";
	
	// ------------------------------------------------------------------------
	
	protected DataStatistics dataStats;
	
	protected PactCompiler withStatsCompiler;
	
	protected PactCompiler noStatsCompiler;
	
	protected InstanceTypeDescription instanceType;
	
	private int statCounter;
	
	// ------------------------------------------------------------------------	
	
	@Before
	public void setup() {
		InetSocketAddress dummyAddr = new InetSocketAddress("localhost", 12345);
		
		this.dataStats = new DataStatistics();
		this.withStatsCompiler = new PactCompiler(this.dataStats, new DefaultCostEstimator(), dummyAddr);
		this.withStatsCompiler.setDefaultDegreeOfParallelism(DEFAULT_PARALLELISM);
		
		this.noStatsCompiler = new PactCompiler(null, new DefaultCostEstimator(), dummyAddr);
		this.noStatsCompiler.setDefaultDegreeOfParallelism(DEFAULT_PARALLELISM);
		
		// create the instance type description
		InstanceType iType = InstanceTypeFactory.construct("standard", 6, 2, 4096, 100, 0);
		HardwareDescription hDesc = HardwareDescriptionFactory.construct(2, 4096 * 1024 * 1024, 2000 * 1024 * 1024);
		this.instanceType = InstanceTypeDescriptionFactory.construct(iType, hDesc, DEFAULT_PARALLELISM * 2);
	}
	
	// ------------------------------------------------------------------------
	
	public OptimizedPlan compileWithStats(Plan p) {
		return this.withStatsCompiler.compile(p, this.instanceType);
	}
	
	public OptimizedPlan compileNoStats(Plan p) {
		return this.noStatsCompiler.compile(p, this.instanceType);
	}
	
	public void setSourceStatistics(GenericDataSource<?> source, long size, float recordWidth) {
		setSourceStatistics(source, new FileBaseStatistics(Long.MAX_VALUE, size, recordWidth));
	}
	
	public void setSourceStatistics(GenericDataSource<?> source, FileBaseStatistics stats) {
		final String key = CACHE_KEY + this.statCounter++;
		this.dataStats.cacheBaseStatistics(stats, key);
		source.setStatisticsKey(key);
	}

	public static ContractResolver getContractResolver(Plan plan) {
		return new ContractResolver(plan);
	}
	
	public static OptimizerPlanNodeResolver getOptimizerPlanNodeResolver(OptimizedPlan plan) {
		return new OptimizerPlanNodeResolver(plan);
	}
	
	// ------------------------------------------------------------------------
	
	public static final class OptimizerPlanNodeResolver {
		
		private final Map<String, ArrayList<PlanNode>> map;
		
		OptimizerPlanNodeResolver(OptimizedPlan p) {
			HashMap<String, ArrayList<PlanNode>> map = new HashMap<String, ArrayList<PlanNode>>();
			
			for (PlanNode n : p.getAllNodes()) {
				Contract c = n.getOriginalOptimizerNode().getPactContract();
				String name = c.getName();
				
				ArrayList<PlanNode> list = map.get(name);
				if (list == null) {
					list = new ArrayList<PlanNode>(2);
					map.put(name, list);
				}
				
				// check whether this node is a child of a node with the same contract (aka combiner)
				boolean shouldAdd = true;
				for (Iterator<PlanNode> iter = list.iterator(); iter.hasNext();) {
					PlanNode in = iter.next();
					if (in.getOriginalOptimizerNode().getPactContract() == c) {
						// is this the child or is our node the child
						if (in instanceof SingleInputPlanNode && n instanceof SingleInputPlanNode) {
							SingleInputPlanNode thisNode = (SingleInputPlanNode) n;
							SingleInputPlanNode otherNode = (SingleInputPlanNode) in;
							
							if (thisNode.getPredecessor() == otherNode) {
								// other node is child, remove it
								iter.remove();
							} else if (otherNode.getPredecessor() == thisNode) {
								shouldAdd = false;
							}
						} else {
							throw new RuntimeException("Unrecodnized case in test.");
						}
					}
				}
				
				if (shouldAdd) {
					list.add(n);
				}
			}
			
			this.map = map;
		}
		
		
		@SuppressWarnings("unchecked")
		public <T extends PlanNode> T getNode(String name) {
			List<PlanNode> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No node found with the given name.");
			} else if (nodes.size() != 1) {
				throw new RuntimeException("Multiple nodes found with the given name.");
			} else {
				return (T) nodes.get(0);
			}
		}
		
		@SuppressWarnings("unchecked")
		public <T extends PlanNode> T getNode(String name, Class<? extends Stub> stubClass) {
			List<PlanNode> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No node found with the given name and stub class.");
			} else {
				PlanNode found = null;
				for (PlanNode node : nodes) {
					if (node.getClass() == stubClass) {
						if (found == null) {
							found = node;
						} else {
							throw new RuntimeException("Multiple nodes found with the given name and stub class.");
						}
					}
				}
				if (found == null) {
					throw new RuntimeException("No node found with the given name and stub class.");
				} else {
					return (T) found;
				}
			}
		}
		
		public List<PlanNode> getNodes(String name) {
			List<PlanNode> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No node found with the given name.");
			} else {
				return new ArrayList<PlanNode>(nodes);
			}
		}
	}
	
	// ------------------------------------------------------------------------
	
	public static final class ContractResolver implements Visitor<Contract> {
		
		private final Map<String, List<Contract>> map;
		private Set<Contract> seen;
		
		ContractResolver(Plan p) {
			this.map = new HashMap<String, List<Contract>>();
			this.seen = new HashSet<Contract>();
			
			p.accept(this);
			this.seen = null;
		}
		
		
		@SuppressWarnings("unchecked")
		public <T extends Contract> T getNode(String name) {
			List<Contract> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No nodes found with the given name.");
			} else if (nodes.size() != 1) {
				throw new RuntimeException("Multiple nodes found with the given name.");
			} else {
				return (T) nodes.get(0);
			}
		}
		
		@SuppressWarnings("unchecked")
		public <T extends Contract> T getNode(String name, Class<? extends Stub> stubClass) {
			List<Contract> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No node found with the given name and stub class.");
			} else {
				Contract found = null;
				for (Contract node : nodes) {
					if (node.getClass() == stubClass) {
						if (found == null) {
							found = node;
						} else {
							throw new RuntimeException("Multiple nodes found with the given name and stub class.");
						}
					}
				}
				if (found == null) {
					throw new RuntimeException("No node found with the given name and stub class.");
				} else {
					return (T) found;
				}
			}
		}
		
		public List<Contract> getNodes(String name) {
			List<Contract> nodes = this.map.get(name);
			if (nodes == null || nodes.isEmpty()) {
				throw new RuntimeException("No node found with the given name.");
			} else {
				return new ArrayList<Contract>(nodes);
			}
		}

		@Override
		public boolean preVisit(Contract visitable) {
			if (this.seen.add(visitable)) {
				// add to  the map
				final String name = visitable.getName();
				List<Contract> list = this.map.get(name);
				if (list == null) {
					list = new ArrayList<Contract>(2);
					this.map.put(name, list);
				}
				list.add(visitable);
				
				// recurse into bulk iterations
				if (visitable instanceof BulkIteration) {
					((BulkIteration) visitable).getNextPartialSolution().accept(this);
				} else if (visitable instanceof WorksetIteration) {
					((WorksetIteration) visitable).getSolutionSetDelta().accept(this);
					((WorksetIteration) visitable).getNextWorkset().accept(this);
				}
				
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void postVisit(Contract visitable) {}
	}
	
	// ------------------------------------------------------------------------
	
	private static final boolean isWindows() {
		return System.getProperty("os.name").startsWith("Windows");
	}
}
