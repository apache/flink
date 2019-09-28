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

package org.apache.flink.optimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.optimizer.traversals.BinaryUnionReplacer;
import org.apache.flink.optimizer.traversals.BranchesVisitor;
import org.apache.flink.optimizer.traversals.GraphCreatingVisitor;
import org.apache.flink.optimizer.traversals.IdAndEstimatesVisitor;
import org.apache.flink.optimizer.traversals.InterestingPropertyVisitor;
import org.apache.flink.optimizer.traversals.PlanFinalizer;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.Plan;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.SinkJoiner;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SinkJoinerPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.postpass.OptimizerPostPass;
import org.apache.flink.optimizer.traversals.RangePartitionRewriter;
import org.apache.flink.optimizer.traversals.UnionParallelismAndForwardEnforcer;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The optimizer that takes the user specified program plan and creates an optimized plan that contains
 * exact descriptions about how the physical execution will take place. It first translates the user
 * program into an internal optimizer representation and then chooses between different alternatives
 * for shipping strategies and local strategies.
 * <p>
 * The basic principle is taken from optimizer works in systems such as Volcano/Cascades and Selinger/System-R/DB2. The
 * optimizer walks from the sinks down, generating interesting properties, and ascends from the sources generating
 * alternative plans, pruning against the interesting properties.
 * <p>
 * The optimizer also assigns the memory to the individual tasks. This is currently done in a very simple fashion: All
 * sub-tasks that need memory (e.g. reduce or join) are given an equal share of memory.
 */
public class Optimizer {

	// ------------------------------------------------------------------------
	// Constants
	// ------------------------------------------------------------------------

	/**
	 * Compiler hint key for the input channel's shipping strategy. This String is a key to the operator's stub
	 * parameters. The corresponding value tells the compiler which shipping strategy to use for the input channel.
	 * If the operator has two input channels, the shipping strategy is applied to both input channels.
	 */
	public static final String HINT_SHIP_STRATEGY = "INPUT_SHIP_STRATEGY";

	/**
	 * Compiler hint key for the <b>first</b> input channel's shipping strategy. This String is a key to
	 * the operator's stub parameters. The corresponding value tells the compiler which shipping strategy
	 * to use for the <b>first</b> input channel. Only applicable to operators with two inputs.
	 */
	public static final String HINT_SHIP_STRATEGY_FIRST_INPUT = "INPUT_LEFT_SHIP_STRATEGY";

	/**
	 * Compiler hint key for the <b>second</b> input channel's shipping strategy. This String is a key to
	 * the operator's stub parameters. The corresponding value tells the compiler which shipping strategy
	 * to use for the <b>second</b> input channel. Only applicable to operators with two inputs.
	 */
	public static final String HINT_SHIP_STRATEGY_SECOND_INPUT = "INPUT_RIGHT_SHIP_STRATEGY";

	/**
	 * Value for the shipping strategy compiler hint that enforces a <b>Forward</b> strategy on the
	 * input channel, i.e. no redistribution of any kind.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_FORWARD = "SHIP_FORWARD";
	
	/**
	 * Value for the shipping strategy compiler hint that enforces a random repartition strategy.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_REPARTITION= "SHIP_REPARTITION";
	
	/**
	 * Value for the shipping strategy compiler hint that enforces a hash-partition strategy.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_REPARTITION_HASH = "SHIP_REPARTITION_HASH";
	
	/**
	 * Value for the shipping strategy compiler hint that enforces a range-partition strategy.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_REPARTITION_RANGE = "SHIP_REPARTITION_RANGE";

	/**
	 * Value for the shipping strategy compiler hint that enforces a <b>broadcast</b> strategy on the
	 * input channel.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_BROADCAST = "SHIP_BROADCAST";

	/**
	 * Compiler hint key for the operator's local strategy. This String is a key to the operator's stub
	 * parameters. The corresponding value tells the compiler which local strategy to use to process the
	 * data inside one partition.
	 * <p>
	 * This hint is ignored by operators that do not have a local strategy (such as <i>Map</i>), or by operators that
	 * have no choice in their local strategy (such as <i>Cross</i>).
	 */
	public static final String HINT_LOCAL_STRATEGY = "LOCAL_STRATEGY";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort based</b> local strategy.
	 * For example, a <i>Reduce</i> operator will sort the data to group it.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT = "LOCAL_STRATEGY_SORT";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort based</b> local strategy.
	 * During sorting a combine method is repeatedly applied to reduce the data volume.
	 * For example, a <i>Reduce</i> operator will sort the data to group it.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_COMBINING_SORT = "LOCAL_STRATEGY_COMBINING_SORT";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy on both
	 * inputs with subsequent merging of inputs. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> operator will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE = "LOCAL_STRATEGY_SORT_BOTH_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy.
	 * The first input is sorted, the second input is assumed to be sorted. After sorting both inputs are merged.
	 * For example, a <i>Match</i> or <i>CoGroup</i> operator will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE = "LOCAL_STRATEGY_SORT_FIRST_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy.
	 * The second input is sorted, the first input is assumed to be sorted. After sorting both inputs are merged.
	 * For example, a <i>Match</i> or <i>CoGroup</i> operator will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE = "LOCAL_STRATEGY_SORT_SECOND_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>merge based</b> local strategy.
	 * Both inputs are assumed to be sorted and are merged. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> operator will use a merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_MERGE = "LOCAL_STRATEGY_MERGE";

	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>hash based</b> local strategy.
	 * For example, a <i>Match</i> operator will use a hybrid-hash-join strategy to find pairs of
	 * matching keys. The <b>first</b> input will be used to build the hash table, the second input will be
	 * used to probe the table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST = "LOCAL_STRATEGY_HASH_BUILD_FIRST";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>hash based</b> local strategy.
	 * For example, a <i>Match</i> operator will use a hybrid-hash-join strategy to find pairs of
	 * matching keys. The <b>second</b> input will be used to build the hash table, the first input will be
	 * used to probe the table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND = "LOCAL_STRATEGY_HASH_BUILD_SECOND";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> operator will process the data of the <b>first</b> input in the outer-loop of the nested loops.
	 * Hence, the data of the first input will be is streamed though, while the data of the second input is stored on
	 * disk
	 * and repeatedly read.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST = "LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> operator will process the data of the <b>second</b> input in the outer-loop of the nested loops.
	 * Hence, the data of the second input will be is streamed though, while the data of the first input is stored on
	 * disk
	 * and repeatedly read.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND = "LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> operator will process the data of the <b>first</b> input in the outer-loop of the nested loops.
	 * Further more, the first input, being the outer side, will be processed in blocks, and for each block, the second
	 * input,
	 * being the inner side, will read repeatedly from disk.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST = "LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> operator will process the data of the <b>second</b> input in the outer-loop of the nested loops.
	 * Further more, the second input, being the outer side, will be processed in blocks, and for each block, the first
	 * input,
	 * being the inner side, will read repeatedly from disk.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND = "LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND";
	
	/**
	 * The log handle that is used by the compiler to log messages.
	 */
	public static final Logger LOG = LoggerFactory.getLogger(Optimizer.class);

	// ------------------------------------------------------------------------
	// Members
	// ------------------------------------------------------------------------

	/**
	 * The statistics object used to obtain statistics, such as input sizes,
	 * for the cost estimation process.
	 */
	private final DataStatistics statistics;

	/**
	 * The cost estimator used by the compiler.
	 */
	private final CostEstimator costEstimator;

	/**
	 * The default parallelism for jobs compiled by this compiler.
	 */
	private int defaultParallelism;


	// ------------------------------------------------------------------------
	// Constructor & Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new optimizer instance. The optimizer has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and hence use only the heuristic cost functions, which result in the selection
	 * of the most robust execution strategies.
	 */
	public Optimizer(Configuration config) {
		this(null, new DefaultCostEstimator(), config);
	}

	/**
	 * Creates a new optimizer instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the optimizer can make better choices for the execution strategies.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 */
	public Optimizer(DataStatistics stats, Configuration config) {
		this(stats, new DefaultCostEstimator(), config);
	}

	/**
	 * Creates a new optimizer instance. The optimizer has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and hence use only the heuristic cost functions, which result in the selection
	 * of the most robust execution strategies.
	 *
	 * The optimizer uses the given cost estimator to compute the costs of the individual operations.
	 * 
	 * @param estimator The cost estimator to use to cost the individual operations.
	 */
	public Optimizer(CostEstimator estimator, Configuration config) {
		this(null, estimator, config);
	}

	/**
	 * Creates a new optimizer instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the optimizer can make better choices for the execution strategies.
	 *
	 * The optimizer uses the given cost estimator to compute the costs of the individual operations.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 * @param estimator
	 *        The <tt>CostEstimator</tt> to use to cost the individual operations.
	 */
	public Optimizer(DataStatistics stats, CostEstimator estimator, Configuration config) {
		this.statistics = stats;
		this.costEstimator = estimator;

		// determine the default parallelism
		this.defaultParallelism = config.getInteger(CoreOptions.DEFAULT_PARALLELISM);

		if (defaultParallelism < 1) {
			this.defaultParallelism = CoreOptions.DEFAULT_PARALLELISM.defaultValue();
			LOG.warn("Config value {} for option {} is invalid. Ignoring and using a value of {}.",
				defaultParallelism,
				CoreOptions.DEFAULT_PARALLELISM.key(),
				defaultParallelism);
		}
	}
	
	// ------------------------------------------------------------------------
	//                             Getters / Setters
	// ------------------------------------------------------------------------
	
	public int getDefaultParallelism() {
		return defaultParallelism;
	}
	
	public void setDefaultParallelism(int defaultParallelism) {
		if (defaultParallelism > 0) {
			this.defaultParallelism = defaultParallelism;
		} else {
			throw new IllegalArgumentException("Default parallelism cannot be zero or negative.");
		}
	}
	
	// ------------------------------------------------------------------------
	//                               Compilation
	// ------------------------------------------------------------------------

	/**
	 * Translates the given program to an OptimizedPlan, where all nodes have their local strategy assigned
	 * and all channels have a shipping strategy assigned.
	 *
	 * For more details on the optimization phase, see the comments for
	 * {@link #compile(org.apache.flink.api.common.Plan, org.apache.flink.optimizer.postpass.OptimizerPostPass)}.
	 * 
	 * @param program The program to be translated.
	 * @return The optimized plan.
	 *
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	public OptimizedPlan compile(Plan program) throws CompilerException {
		final OptimizerPostPass postPasser = getPostPassFromPlan(program);
		return compile(program, postPasser);
	}

	/**
	 * Translates the given program to an OptimizedPlan. The optimized plan describes for each operator
	 * which strategy to use (such as hash join versus sort-merge join), what data exchange method to use
	 * (local pipe forward, shuffle, broadcast), what exchange mode to use (pipelined, batch),
	 * where to cache intermediate results, etc,
	 *
	 * The optimization happens in multiple phases:
	 * <ol>
	 *     <li>Create optimizer dag implementation of the program.
	 *
	 *     <tt>OptimizerNode</tt> representations of the PACTs, assign parallelism and compute size estimates.</li>
	 * <li>Compute interesting properties and auxiliary structures.</li>
	 * <li>Enumerate plan alternatives. This cannot be done in the same step as the interesting property computation (as
	 * opposed to the Database approaches), because we support plans that are not trees.</li>
	 * </ol>
	 * 
	 * @param program The program to be translated.
	 * @param postPasser The function to be used for post passing the optimizer's plan and setting the
	 *                   data type specific serialization routines.
	 * @return The optimized plan.
	 * 
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	private OptimizedPlan compile(Plan program, OptimizerPostPass postPasser) throws CompilerException {
		if (program == null || postPasser == null) {
			throw new NullPointerException();
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Beginning compilation of program '" + program.getJobName() + '\'');
		}

		final ExecutionMode defaultDataExchangeMode = program.getExecutionConfig().getExecutionMode();

		final int defaultParallelism = program.getDefaultParallelism() > 0 ?
			program.getDefaultParallelism() : this.defaultParallelism;

		// log the default settings
		LOG.debug("Using a default parallelism of {}",  defaultParallelism);
		LOG.debug("Using default data exchange mode {}", defaultDataExchangeMode);

		// the first step in the compilation is to create the optimizer plan representation
		// this step does the following:
		// 1) It creates an optimizer plan node for each operator
		// 2) It connects them via channels
		// 3) It looks for hints about local strategies and channel types and
		// sets the types and strategies accordingly
		// 4) It makes estimates about the data volume of the data sources and
		// propagates those estimates through the plan

		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(defaultParallelism, defaultDataExchangeMode);
		program.accept(graphCreator);

		// if we have a plan with multiple data sinks, add logical optimizer nodes that have two data-sinks as children
		// each until we have only a single root node. This allows to transparently deal with the nodes with
		// multiple outputs
		OptimizerNode rootNode;
		if (graphCreator.getSinks().size() == 1) {
			rootNode = graphCreator.getSinks().get(0);
		}
		else if (graphCreator.getSinks().size() > 1) {
			Iterator<DataSinkNode> iter = graphCreator.getSinks().iterator();
			rootNode = iter.next();

			while (iter.hasNext()) {
				rootNode = new SinkJoiner(rootNode, iter.next());
			}
		}
		else {
			throw new CompilerException("Bug: The optimizer plan representation has no sinks.");
		}

		// now that we have all nodes created and recorded which ones consume memory, tell the nodes their minimal
		// guaranteed memory, for further cost estimations. We assume an equal distribution of memory among consumer tasks
		rootNode.accept(new IdAndEstimatesVisitor(this.statistics));

		// We need to enforce that union nodes always forward their output to their successor.
		// Any partitioning must be either pushed before or done after the union, but not on the union's output.
		UnionParallelismAndForwardEnforcer unionEnforcer = new UnionParallelismAndForwardEnforcer();
		rootNode.accept(unionEnforcer);

		// We are dealing with operator DAGs, rather than operator trees.
		// That requires us to deviate at some points from the classical DB optimizer algorithms.
		// This step builds auxiliary structures to help track branches and joins in the DAG
		BranchesVisitor branchingVisitor = new BranchesVisitor();
		rootNode.accept(branchingVisitor);

		// Propagate the interesting properties top-down through the graph
		InterestingPropertyVisitor propsVisitor = new InterestingPropertyVisitor(this.costEstimator);
		rootNode.accept(propsVisitor);
		
		// perform a sanity check: the root may not have any unclosed branches
		if (rootNode.getOpenBranches() != null && rootNode.getOpenBranches().size() > 0) {
			throw new CompilerException("Bug: Logic for branching plans (non-tree plans) has an error, and does not " +
					"track the re-joining of branches correctly.");
		}

		// the final step is now to generate the actual plan alternatives
		List<PlanNode> bestPlan = rootNode.getAlternativePlans(this.costEstimator);

		if (bestPlan.size() != 1) {
			throw new CompilerException("Error in compiler: more than one best plan was created!");
		}

		// check if the best plan's root is a data sink (single sink plan)
		// if so, directly take it. if it is a sink joiner node, get its contained sinks
		PlanNode bestPlanRoot = bestPlan.get(0);
		List<SinkPlanNode> bestPlanSinks = new ArrayList<SinkPlanNode>(4);

		if (bestPlanRoot instanceof SinkPlanNode) {
			bestPlanSinks.add((SinkPlanNode) bestPlanRoot);
		} else if (bestPlanRoot instanceof SinkJoinerPlanNode) {
			((SinkJoinerPlanNode) bestPlanRoot).getDataSinks(bestPlanSinks);
		}

		// finalize the plan
		OptimizedPlan plan = new PlanFinalizer().createFinalPlan(bestPlanSinks, program.getJobName(), program);
		
		plan.accept(new BinaryUnionReplacer());

		plan.accept(new RangePartitionRewriter(plan));

		// post pass the plan. this is the phase where the serialization and comparator code is set
		postPasser.postPass(plan);
		
		return plan;
	}

	/**
	 * This function performs only the first step to the compilation process - the creation of the optimizer
	 * representation of the plan. No estimations or enumerations of alternatives are done here.
	 * 
	 * @param program The plan to generate the optimizer representation for.
	 * @return The optimizer representation of the plan, as a collection of all data sinks
	 *         from the plan can be traversed.
	 */
	public static List<DataSinkNode> createPreOptimizedPlan(Plan program) {
		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(1, null);
		program.accept(graphCreator);
		return graphCreator.getSinks();
	}


	// ------------------------------------------------------------------------
	// Miscellaneous
	// ------------------------------------------------------------------------
	
	private OptimizerPostPass getPostPassFromPlan(Plan program) {
		final String className = program.getPostPassClassName();
		if (className == null) {
			throw new CompilerException("Optimizer Post Pass class description is null");
		}
		try {
			Class<? extends OptimizerPostPass> clazz = Class.forName(className).asSubclass(OptimizerPostPass.class);
			try {
				return InstantiationUtil.instantiate(clazz, OptimizerPostPass.class);
			} catch (RuntimeException rtex) {
				// unwrap the source exception
				if (rtex.getCause() != null) {
					throw new CompilerException("Cannot instantiate optimizer post pass: " + rtex.getMessage(), rtex.getCause());
				} else {
					throw rtex;
				}
			}
		}
		catch (ClassNotFoundException cnfex) {
			throw new CompilerException("Cannot load Optimizer post-pass class '" + className + "'.", cnfex);
		}
		catch (ClassCastException ccex) {
			throw new CompilerException("Class '" + className + "' is not an optimizer post-pass.", ccex);
		}
	}
}
