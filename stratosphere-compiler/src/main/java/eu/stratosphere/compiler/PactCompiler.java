/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.api.common.operators.base.*;
import eu.stratosphere.compiler.dag.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.Union;
import eu.stratosphere.api.common.operators.base.BulkIterationBase.PartialSolutionPlaceHolder;
import eu.stratosphere.api.common.operators.base.DeltaIterationBase.SolutionSetPlaceHolder;
import eu.stratosphere.api.common.operators.base.DeltaIterationBase.WorksetPlaceHolder;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.costs.DefaultCostEstimator;
import eu.stratosphere.compiler.deadlockdetect.DeadlockPreventer;
import eu.stratosphere.compiler.plan.BinaryUnionPlanNode;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.BulkPartialSolutionPlanNode;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.IterationPlanNode;
import eu.stratosphere.compiler.plan.NAryUnionPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SinkJoinerPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plan.SolutionSetPlanNode;
import eu.stratosphere.compiler.plan.SourcePlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.compiler.plan.WorksetPlanNode;
import eu.stratosphere.compiler.postpass.OptimizerPostPass;
import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.util.InstantiationUtil;
import eu.stratosphere.util.Visitor;

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
public class PactCompiler {

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
	 * The the first input is sorted, the second input is assumed to be sorted. After sorting both inputs are merged. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> operator will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE = "LOCAL_STRATEGY_SORT_FIRST_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy.
	 * The the second input is sorted, the first input is assumed to be sorted. After sorting both inputs are merged. 
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
	public static final Log LOG = LogFactory.getLog(PactCompiler.class);

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
	 * The default degree of parallelism for jobs compiled by this compiler.
	 */
	private int defaultDegreeOfParallelism;


	// ------------------------------------------------------------------------
	// Constructor & Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust execution strategies. The
	 * compiler also uses conservative default estimates for the operator costs, since
	 * it has no access to another cost estimator.
	 * <p>
	 * The address of the job manager (to obtain system characteristics) is determined via the global configuration.
	 */
	public PactCompiler() {
		this(null, new DefaultCostEstimator());
	}

	/**
	 * Creates a new compiler instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the compiler can make better choices for the execution strategies.
	 * as if no filesystem was given. The compiler uses conservative default estimates for the operator costs, since
	 * it has no access to another cost estimator.
	 * <p>
	 * The address of the job manager (to obtain system characteristics) is determined via the global configuration.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 */
	public PactCompiler(DataStatistics stats) {
		this(stats, new DefaultCostEstimator());
	}

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust execution strategies. It uses
	 * however the given cost estimator to compute the costs of the individual operations.
	 * <p>
	 * The address of the job manager (to obtain system characteristics) is determined via the global configuration.
	 * 
	 * @param estimator
	 *        The <tt>CostEstimator</tt> to use to cost the individual operations.
	 */
	public PactCompiler(CostEstimator estimator) {
		this(null, estimator);
	}

	/**
	 * Creates a new compiler instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the compiler can make better choices for the execution strategies.
	 * as if no filesystem was given. It uses the given cost estimator to compute the costs of the individual
	 * operations.
	 * <p>
	 * The address of the job manager (to obtain system characteristics) is determined via the global configuration.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 * @param estimator
	 *        The <tt>CostEstimator</tt> to use to cost the individual operations.
	 */
	public PactCompiler(DataStatistics stats, CostEstimator estimator) {
		this.statistics = stats;
		this.costEstimator = estimator;

		Configuration config = GlobalConfiguration.getConfiguration();

		// determine the default parallelization degree
		this.defaultDegreeOfParallelism = config.getInteger(ConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY,
			ConfigConstants.DEFAULT_PARALLELIZATION_DEGREE);
	}
	
	// ------------------------------------------------------------------------
	//                             Getters / Setters
	// ------------------------------------------------------------------------
	
	public int getDefaultDegreeOfParallelism() {
		return defaultDegreeOfParallelism;
	}
	
	public void setDefaultDegreeOfParallelism(int defaultDegreeOfParallelism) {
		if (defaultDegreeOfParallelism > 0) {
			this.defaultDegreeOfParallelism = defaultDegreeOfParallelism;
		} else {
			throw new IllegalArgumentException("Default parallelism cannot be zero or negative.");
		}
	}
	
	// ------------------------------------------------------------------------
	//                               Compilation
	// ------------------------------------------------------------------------

	/**
	 * Translates the given plan in to an OptimizedPlan, where all nodes have their local strategy assigned
	 * and all channels have a shipping strategy assigned. The compiler connects to the job manager to obtain information
	 * about the available instances and their memory and then chooses an instance type to schedule the execution on.
	 * <p>
	 * The compilation process itself goes through several phases:
	 * <ol>
	 * <li>Create an optimizer data flow representation of the program, assign parallelism and compute size estimates.</li>
	 * <li>Compute interesting properties and auxiliary structures.</li>
	 * <li>Enumerate plan alternatives. This cannot be done in the same step as the interesting property computation (as
	 * opposed to the Database approaches), because we support plans that are not trees.</li>
	 * </ol>
	 * 
	 * @param program The program to be translated.
	 * @return The optimized plan.
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	public OptimizedPlan compile(Plan program) throws CompilerException {
		// -------------------- try to get the connection to the job manager ----------------------
		// --------------------------to obtain instance information --------------------------------
		final OptimizerPostPass postPasser = getPostPassFromPlan(program);
		return compile(program, postPasser);
	}

	/**
	 * Translates the given pact plan in to an OptimizedPlan, where all nodes have their local strategy assigned
	 * and all channels have a shipping strategy assigned. The process goes through several phases:
	 * <ol>
	 * <li>Create <tt>OptimizerNode</tt> representations of the PACTs, assign parallelism and compute size estimates.</li>
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

		// set the default degree of parallelism
		int defaultParallelism = program.getDefaultParallelism() > 0 ?
			program.getDefaultParallelism() : this.defaultDegreeOfParallelism;

		// log the output
		if (LOG.isDebugEnabled()) {
			LOG.debug("Using a default degree of parallelism of " + defaultParallelism + '.');
		}

		// the first step in the compilation is to create the optimizer plan representation
		// this step does the following:
		// 1) It creates an optimizer plan node for each operator
		// 2) It connects them via channels
		// 3) It looks for hints about local strategies and channel types and
		// sets the types and strategies accordingly
		// 4) It makes estimates about the data volume of the data sources and
		// propagates those estimates through the plan

		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(defaultParallelism);
		program.accept(graphCreator);

		// if we have a plan with multiple data sinks, add logical optimizer nodes that have two data-sinks as children
		// each until we have only a single root node. This allows to transparently deal with the nodes with
		// multiple outputs
		OptimizerNode rootNode;
		if (graphCreator.sinks.size() == 1) {
			rootNode = graphCreator.sinks.get(0);
		} else if (graphCreator.sinks.size() > 1) {
			Iterator<DataSinkNode> iter = graphCreator.sinks.iterator();
			rootNode = iter.next();

			while (iter.hasNext()) {
				rootNode = new SinkJoiner(rootNode, iter.next());
			}
		} else {
			throw new CompilerException("Bug: The optimizer plan representation has no sinks.");
		}

		// now that we have all nodes created and recorded which ones consume memory, tell the nodes their minimal
		// guaranteed memory, for further cost estimations. we assume an equal distribution of memory among consumer tasks
		
		rootNode.accept(new IdAndEstimatesVisitor(this.statistics));
		
		// Now that the previous step is done, the next step is to traverse the graph again for the two
		// steps that cannot directly be performed during the plan enumeration, because we are dealing with DAGs
		// rather than a trees. That requires us to deviate at some points from the classical DB optimizer algorithms.
		//
		// 1) propagate the interesting properties top-down through the graph
		// 2) Track information about nodes with multiple outputs that are later on reconnected in a node with
		// multiple inputs.
		InterestingPropertyVisitor propsVisitor = new InterestingPropertyVisitor(this.costEstimator);
		rootNode.accept(propsVisitor);
		
		BranchesVisitor branchingVisitor = new BranchesVisitor();
		rootNode.accept(branchingVisitor);
		
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
		
		DeadlockPreventer dp = new DeadlockPreventer();
		dp.resolveDeadlocks(bestPlanSinks);

		// finalize the plan
		OptimizedPlan plan = new PlanFinalizer().createFinalPlan(bestPlanSinks, program.getJobName(), program);

		// swap the binary unions for n-ary unions. this changes no strategies or memory consumers whatsoever, so
		// we can do this after the plan finalization
		plan.accept(new BinaryUnionReplacer());
		
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
		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(1);
		program.accept(graphCreator);
		return graphCreator.sinks;
	}
	
	// ------------------------------------------------------------------------
	//                 Visitors for Compilation Traversals
	// ------------------------------------------------------------------------
	
	/**
	 * This utility class performs the translation from the user specified program to the optimizer plan.
	 * It works as a visitor that walks the user's job in a depth-first fashion. During the descend, it creates
	 * an optimizer node for each operator, respectively data source or -sink. During the ascend, it connects
	 * the nodes to the full graph.
	 * <p>
	 * This translator relies on the <code>setInputs</code> method in the nodes. As that method implements the size
	 * estimation and the awareness for optimizer hints, the sizes will be properly estimated and the translated plan
	 * already respects all optimizer hints.
	 */
	private static final class GraphCreatingVisitor implements Visitor<Operator<?>> {
		
		private final Map<Operator<?>, OptimizerNode> con2node; // map from the operator objects to their
																// corresponding optimizer nodes

		private final List<DataSourceNode> sources; // all data source nodes in the optimizer plan

		private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

		private final int defaultParallelism; // the default degree of parallelism
		
		private final GraphCreatingVisitor parent;	// reference to enclosing creator, in case of a recursive translation
		
		private final boolean forceDOP;

		
		private GraphCreatingVisitor(int defaultParallelism) {
			this(null, false, defaultParallelism, null);
		}

		private GraphCreatingVisitor(GraphCreatingVisitor parent, boolean forceDOP,
									int defaultParallelism, HashMap<Operator<?>, OptimizerNode> closure) {
			if (closure == null){
				con2node = new HashMap<Operator<?>, OptimizerNode>();
			} else {
				con2node = closure;
			}
			this.sources = new ArrayList<DataSourceNode>(4);
			this.sinks = new ArrayList<DataSinkNode>(2);
			this.defaultParallelism = defaultParallelism;
			this.parent = parent;
			this.forceDOP = forceDOP;
		}

		@Override
		public boolean preVisit(Operator<?> c) {
			// check if we have been here before
			if (this.con2node.containsKey(c)) {
				return false;
			}

			final OptimizerNode n;

			// create a node for the operator (or sink or source) if we have not been here before
			if (c instanceof GenericDataSinkBase) {
				DataSinkNode dsn = new DataSinkNode((GenericDataSinkBase<?>) c);
				this.sinks.add(dsn);
				n = dsn;
			}
			else if (c instanceof GenericDataSourceBase) {
				DataSourceNode dsn = new DataSourceNode((GenericDataSourceBase<?, ?>) c);
				this.sources.add(dsn);
				n = dsn;
			}
			else if (c instanceof MapOperatorBase) {
				n = new MapNode((MapOperatorBase<?, ?, ?>) c);
			}
            else if (c instanceof MapPartitionOperatorBase) {
                n = new MapPartitionNode((MapPartitionOperatorBase<?, ?, ?>) c);
            }
			else if (c instanceof CollectorMapOperatorBase) {
				n = new CollectorMapNode((CollectorMapOperatorBase<?, ?, ?>) c);
			}
			else if (c instanceof FlatMapOperatorBase) {
				n = new FlatMapNode((FlatMapOperatorBase<?, ?, ?>) c);
			}
			else if (c instanceof FilterOperatorBase) {
				n = new FilterNode((FilterOperatorBase<?, ?>) c);
			}
			else if (c instanceof ReduceOperatorBase) {
				n = new ReduceNode((ReduceOperatorBase<?, ?>) c);
			}
			else if (c instanceof GroupReduceOperatorBase) {
				n = new GroupReduceNode((GroupReduceOperatorBase<?, ?, ?>) c);
			}
			else if (c instanceof JoinOperatorBase) {
				n = new MatchNode((JoinOperatorBase<?, ?, ?, ?>) c);
			}
			else if (c instanceof CoGroupOperatorBase) {
				n = new CoGroupNode((CoGroupOperatorBase<?, ?, ?, ?>) c);
			}
			else if (c instanceof CrossOperatorBase) {
				n = new CrossNode((CrossOperatorBase<?, ?, ?, ?>) c);
			}
			else if (c instanceof BulkIterationBase) {
				n = new BulkIterationNode((BulkIterationBase<?>) c);
			}
			else if (c instanceof DeltaIterationBase) {
				n = new WorksetIterationNode((DeltaIterationBase<?, ?>) c);
			}
			else if (c instanceof Union){
				n = new BinaryUnionNode((Union<?>) c);
			}
			else if (c instanceof PartialSolutionPlaceHolder) {
				final PartialSolutionPlaceHolder<?> holder = (PartialSolutionPlaceHolder<?>) c;
				final BulkIterationBase<?> enclosingIteration = holder.getContainingBulkIteration();
				final BulkIterationNode containingIterationNode =
							(BulkIterationNode) this.parent.con2node.get(enclosingIteration);
				
				// catch this for the recursive translation of step functions
				BulkPartialSolutionNode p = new BulkPartialSolutionNode(holder, containingIterationNode);
				p.setDegreeOfParallelism(containingIterationNode.getDegreeOfParallelism());
				n = p;
			}
			else if (c instanceof WorksetPlaceHolder) {
				final WorksetPlaceHolder<?> holder = (WorksetPlaceHolder<?>) c;
				final DeltaIterationBase<?, ?> enclosingIteration = holder.getContainingWorksetIteration();
				final WorksetIterationNode containingIterationNode =
							(WorksetIterationNode) this.parent.con2node.get(enclosingIteration);
				
				// catch this for the recursive translation of step functions
				WorksetNode p = new WorksetNode(holder, containingIterationNode);
				p.setDegreeOfParallelism(containingIterationNode.getDegreeOfParallelism());
				n = p;
			}
			else if (c instanceof SolutionSetPlaceHolder) {
				final SolutionSetPlaceHolder<?> holder = (SolutionSetPlaceHolder<?>) c;
				final DeltaIterationBase<?, ?> enclosingIteration = holder.getContainingWorksetIteration();
				final WorksetIterationNode containingIterationNode =
							(WorksetIterationNode) this.parent.con2node.get(enclosingIteration);
				
				// catch this for the recursive translation of step functions
				SolutionSetNode p = new SolutionSetNode(holder, containingIterationNode);
				p.setDegreeOfParallelism(containingIterationNode.getDegreeOfParallelism());
				n = p;
			}
			else {
				throw new IllegalArgumentException("Unknown operator type: " + c);
			}

			this.con2node.put(c, n);
			
			// set the parallelism only if it has not been set before. some nodes have a fixed DOP, such as the
			// key-less reducer (all-reduce)
			if (n.getDegreeOfParallelism() < 1) {
				// set the degree of parallelism
				int par = c.getDegreeOfParallelism();
				if (par > 0) {
					if (this.forceDOP && par != this.defaultParallelism) {
						par = this.defaultParallelism;
						LOG.warn("The degree-of-parallelism of nested Dataflows (such as step functions in iterations) is " +
							"currently fixed to the degree-of-parallelism of the surrounding operator (the iteration).");
					}
				} else {
					par = this.defaultParallelism;
				}
				n.setDegreeOfParallelism(par);
			}

			return true;
		}

		@Override
		public void postVisit(Operator<?> c) {
			
			OptimizerNode n = this.con2node.get(c);

			// first connect to the predecessors
			n.setInput(this.con2node);
			n.setBroadcastInputs(this.con2node);
			
			// if the node represents a bulk iteration, we recursively translate the data flow now
			if (n instanceof BulkIterationNode) {
				final BulkIterationNode iterNode = (BulkIterationNode) n;
				final BulkIterationBase<?> iter = iterNode.getIterationContract();

				// calculate closure of the anonymous function
				HashMap<Operator<?>, OptimizerNode> closure = new HashMap<Operator<?>, OptimizerNode>(con2node);

				// first, recursively build the data flow for the step function
				final GraphCreatingVisitor recursiveCreator = new GraphCreatingVisitor(this, true,
					iterNode.getDegreeOfParallelism(), closure);
				
				BulkPartialSolutionNode partialSolution = null;
				
				iter.getNextPartialSolution().accept(recursiveCreator);
				
				partialSolution =  (BulkPartialSolutionNode) recursiveCreator.con2node.get(iter.getPartialSolution());
				OptimizerNode rootOfStepFunction = recursiveCreator.con2node.get(iter.getNextPartialSolution());
				if (partialSolution == null) {
					throw new CompilerException("Error: The step functions result does not depend on the partial solution.");
				}
				
				
				OptimizerNode terminationCriterion = null;
				
				if (iter.getTerminationCriterion() != null) {
					terminationCriterion = recursiveCreator.con2node.get(iter.getTerminationCriterion());
					
					// no intermediate node yet, traverse from the termination criterion to build the missing parts
					if (terminationCriterion == null) {
						iter.getTerminationCriterion().accept(recursiveCreator);
						terminationCriterion = recursiveCreator.con2node.get(iter.getTerminationCriterion());
					}
				}
				
				iterNode.setNextPartialSolution(rootOfStepFunction, terminationCriterion);
				iterNode.setPartialSolution(partialSolution);
				
				// go over the contained data flow and mark the dynamic path nodes
				StaticDynamicPathIdentifier identifier = new StaticDynamicPathIdentifier(iterNode.getCostWeight());
				rootOfStepFunction.accept(identifier);
				if(terminationCriterion != null){
					terminationCriterion.accept(identifier);
				}
			}
			else if (n instanceof WorksetIterationNode) {
				final WorksetIterationNode iterNode = (WorksetIterationNode) n;
				final DeltaIterationBase<?, ?> iter = iterNode.getIterationContract();

				// calculate the closure of the anonymous function
				HashMap<Operator<?>, OptimizerNode> closure = new HashMap<Operator<?>, OptimizerNode>(con2node);

				// first, recursively build the data flow for the step function
				final GraphCreatingVisitor recursiveCreator = new GraphCreatingVisitor(this, true,
					iterNode.getDegreeOfParallelism(), closure);
				// descend from the solution set delta. check that it depends on both the workset
				// and the solution set. If it does depend on both, this descend should create both nodes
				iter.getSolutionSetDelta().accept(recursiveCreator);
				
				final SolutionSetNode solutionSetNode = (SolutionSetNode) recursiveCreator.con2node.get(iter.getSolutionSet());
				final WorksetNode worksetNode = (WorksetNode) recursiveCreator.con2node.get(iter.getWorkset());
				
				if (worksetNode == null) {
					throw new CompilerException("In the given plan, the solution set delta does not depend on the workset. This is a prerequisite in workset iterations.");
				}
				
				iter.getNextWorkset().accept(recursiveCreator);
				
				if (solutionSetNode == null || solutionSetNode.getOutgoingConnections() == null || solutionSetNode.getOutgoingConnections().isEmpty()) {
					throw new CompilerException("Error: The step function does not reference the solution set.");
				} else {
					for (PactConnection conn : solutionSetNode.getOutgoingConnections()) {
						OptimizerNode successor = conn.getTarget();
					
						if (successor.getClass() == MatchNode.class) {
							// find out which input to the match the solution set is
							MatchNode mn = (MatchNode) successor;
							if (mn.getFirstPredecessorNode() == solutionSetNode) {
								mn.makeJoinWithSolutionSet(0);
							} else if (mn.getSecondPredecessorNode() == solutionSetNode) {
								mn.makeJoinWithSolutionSet(1);
							} else {
								throw new CompilerException();
							}
						}
						else if (successor.getClass() == CoGroupNode.class) {
							CoGroupNode cg = (CoGroupNode) successor;
							if (cg.getFirstPredecessorNode() == solutionSetNode) {
								cg.makeCoGroupWithSolutionSet(0);
							} else if (cg.getSecondPredecessorNode() == solutionSetNode) {
								cg.makeCoGroupWithSolutionSet(1);
							} else {
								throw new CompilerException();
							}
						}
						else {
							throw new CompilerException("Error: The only operations allowed on the solution set are Join and CoGroup.");
						}
					}
				}
				
				final OptimizerNode nextWorksetNode = recursiveCreator.con2node.get(iter.getNextWorkset());
				final OptimizerNode solutionSetDeltaNode = recursiveCreator.con2node.get(iter.getSolutionSetDelta());
				
				// set the step function nodes to the iteration node
				iterNode.setPartialSolution(solutionSetNode, worksetNode);
				iterNode.setNextPartialSolution(solutionSetDeltaNode, nextWorksetNode);
				
				// go over the contained data flow and mark the dynamic path nodes
				StaticDynamicPathIdentifier pathIdentifier = new StaticDynamicPathIdentifier(iterNode.getCostWeight());
				nextWorksetNode.accept(pathIdentifier);
				iterNode.getSolutionSetDelta().accept(pathIdentifier);
			}
		}
	};
	
	private static final class StaticDynamicPathIdentifier implements Visitor<OptimizerNode> {
		
		private final Set<OptimizerNode> seenBefore = new HashSet<OptimizerNode>();
		
		private final int costWeight;
		
		private StaticDynamicPathIdentifier(int costWeight) {
			this.costWeight = costWeight;
		}
		
		@Override
		public boolean preVisit(OptimizerNode visitable) {
			return this.seenBefore.add(visitable);
		}

		@Override
		public void postVisit(OptimizerNode visitable) {
			visitable.identifyDynamicPath(this.costWeight);
		}
	}
	
	/**
	 * Simple visitor that sets the minimal guaranteed memory per task based on the amount of available memory,
	 * the number of memory consumers, and on the task's degree of parallelism.
	 */
	private static final class IdAndEstimatesVisitor implements Visitor<OptimizerNode> {
		
		private final DataStatistics statistics;

		private int id = 1;
		
		private IdAndEstimatesVisitor(DataStatistics statistics) {
			this.statistics = statistics;
		}


		@Override
		public boolean preVisit(OptimizerNode visitable) {
			if (visitable.getId() != -1) {
				// been here before
				return false;
			}
			
			return true;
		}


		@Override
		public void postVisit(OptimizerNode visitable) {
			// the node ids
			visitable.initId(this.id++);
			
			// connections need to figure out their maximum path depths
			for (PactConnection conn : visitable.getIncomingConnections()) {
				conn.initMaxDepth();
			}
			for (PactConnection conn : visitable.getBroadcastConnections()) {
				conn.initMaxDepth();
			}
			
			// the estimates
			visitable.computeOutputEstimates(this.statistics);
			
			// if required, recurse into the step function
			if (visitable instanceof IterationNode) {
				((IterationNode) visitable).acceptForStepFunction(this);
			}
		}
	}
	
	/**
	 * Visitor that computes the interesting properties for each node in the plan. On its recursive
	 * depth-first descend, it propagates all interesting properties top-down.
	 */
	public static final class InterestingPropertyVisitor implements Visitor<OptimizerNode> {
		
		private CostEstimator estimator; // the cost estimator for maximal costs of an interesting property

		/**
		 * Creates a new visitor that computes the interesting properties for all nodes in the plan.
		 * It uses the given cost estimator used to compute the maximal costs for an interesting property.
		 * 
		 * @param estimator
		 *        The cost estimator to estimate the maximal costs for interesting properties.
		 */
		public InterestingPropertyVisitor(CostEstimator estimator) {
			this.estimator = estimator;
		}
		
		@Override
		public boolean preVisit(OptimizerNode node) {
			// The interesting properties must be computed on the descend. In case a node has multiple outputs,
			// that computation must happen during the last descend.

			if (node.getInterestingProperties() == null && node.haveAllOutputConnectionInterestingProperties()) {
				node.computeUnionOfInterestingPropertiesFromSuccessors();
				node.computeInterestingPropertiesForInputs(this.estimator);
				return true;
			} else {
				return false;
			}
		}


		@Override
		public void postVisit(OptimizerNode visitable) {}
	}

	/**
	 * On its re-ascend (post visit) this visitor, computes auxiliary maps that are needed to support plans
	 * that are not a minimally connected DAG (Such plans are not trees, but at least one node feeds its
	 * output into more than one other node).
	 */
	private static final class BranchesVisitor implements Visitor<OptimizerNode> {
		
		@Override
		public boolean preVisit(OptimizerNode node) {
			return node.getOpenBranches() == null;
		}

		@Override
		public void postVisit(OptimizerNode node) {
			if (node instanceof IterationNode) {
				((IterationNode) node).acceptForStepFunction(this);
			}

			node.computeUnclosedBranchStack();
		}
	};
	
	/**
	 * Utility class that traverses a plan to collect all nodes and add them to the OptimizedPlan.
	 * Besides collecting all nodes, this traversal assigns the memory to the nodes.
	 */
	private static final class PlanFinalizer implements Visitor<PlanNode> {
		
		private final Set<PlanNode> allNodes; // a set of all nodes in the optimizer plan

		private final List<SourcePlanNode> sources; // all data source nodes in the optimizer plan

		private final List<SinkPlanNode> sinks; // all data sink nodes in the optimizer plan
		
		private final Deque<IterationPlanNode> stackOfIterationNodes;

		private int memoryConsumerWeights; // a counter of all memory consumers

		/**
		 * Creates a new plan finalizer.
		 */
		private PlanFinalizer() {
			this.allNodes = new HashSet<PlanNode>();
			this.sources = new ArrayList<SourcePlanNode>();
			this.sinks = new ArrayList<SinkPlanNode>();
			this.stackOfIterationNodes = new ArrayDeque<IterationPlanNode>();
		}

		private OptimizedPlan createFinalPlan(List<SinkPlanNode> sinks, String jobName, Plan originalPlan) {
			this.memoryConsumerWeights = 0;
			
			// traverse the graph
			for (SinkPlanNode node : sinks) {
				node.accept(this);
			}

			// assign the memory to each node
			if (this.memoryConsumerWeights > 0) {
				for (PlanNode node : this.allNodes) {
					// assign memory to the driver strategy of the node
					final int consumerWeight = node.getMemoryConsumerWeight();
					if (consumerWeight > 0) {
						final double relativeMem = (double)consumerWeight / this.memoryConsumerWeights;
						node.setRelativeMemoryPerSubtask(relativeMem);
						if (LOG.isDebugEnabled()) {
							LOG.debug("Assigned " + relativeMem + " of total memory to each subtask of " +
								node.getPactContract().getName() + ".");
						}
					}
					
					// assign memory to the local and global strategies of the channels
					for (Channel c : node.getInputs()) {
						if (c.getLocalStrategy().dams()) {
							final double relativeMem = 1.0 / this.memoryConsumerWeights;
							c.setRelativeMemoryLocalStrategy(relativeMem);
							if (LOG.isDebugEnabled()) {
								LOG.debug("Assigned " + relativeMem + " of total memory to each local strategy " +
										"instance of " + c + ".");
							}
						}
						if (c.getTempMode() != TempMode.NONE) {
							final double relativeMem = 1.0/ this.memoryConsumerWeights;
							c.setRelativeTempMemory(relativeMem);
							if (LOG.isDebugEnabled()) {
								LOG.debug("Assigned " + relativeMem + " of total memory to each instance of the temp " +
										"table" +
										" " +
										"for " + c + ".");
							}
						}
					}
				}
			}
			return new OptimizedPlan(this.sources, this.sinks, this.allNodes, jobName, originalPlan);
		}

		@Override
		public boolean preVisit(PlanNode visitable) {
			// if we come here again, prevent a further descend
			if (!this.allNodes.add(visitable)) {
				return false;
			}
			
			if (visitable instanceof SinkPlanNode) {
				this.sinks.add((SinkPlanNode) visitable);
			}
			else if (visitable instanceof SourcePlanNode) {
				this.sources.add((SourcePlanNode) visitable);
			}
			else if (visitable instanceof BulkPartialSolutionPlanNode) {
				// tell the partial solution about the iteration node that contains it
				final BulkPartialSolutionPlanNode pspn = (BulkPartialSolutionPlanNode) visitable;
				final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();
				
				// sanity check!
				if (iteration == null || !(iteration instanceof BulkIterationPlanNode)) {
					throw new CompilerException("Bug: Error finalizing the plan. " +
							"Cannot associate the node for a partial solutions with its containing iteration.");
				}
				pspn.setContainingIterationNode((BulkIterationPlanNode) iteration);
			}
			else if (visitable instanceof WorksetPlanNode) {
				// tell the partial solution about the iteration node that contains it
				final WorksetPlanNode wspn = (WorksetPlanNode) visitable;
				final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();
				
				// sanity check!
				if (iteration == null || !(iteration instanceof WorksetIterationPlanNode)) {
					throw new CompilerException("Bug: Error finalizing the plan. " +
							"Cannot associate the node for a partial solutions with its containing iteration.");
				}
				wspn.setContainingIterationNode((WorksetIterationPlanNode) iteration);
			}
			else if (visitable instanceof SolutionSetPlanNode) {
				// tell the partial solution about the iteration node that contains it
				final SolutionSetPlanNode sspn = (SolutionSetPlanNode) visitable;
				final IterationPlanNode iteration = this.stackOfIterationNodes.peekLast();
				
				// sanity check!
				if (iteration == null || !(iteration instanceof WorksetIterationPlanNode)) {
					throw new CompilerException("Bug: Error finalizing the plan. " +
							"Cannot associate the node for a partial solutions with its containing iteration.");
				}
				sspn.setContainingIterationNode((WorksetIterationPlanNode) iteration);
			}
			
			// double-connect the connections. previously, only parents knew their children, because
			// one child candidate could have been referenced by multiple parents.
			for (Channel conn : visitable.getInputs()) {
				conn.setTarget(visitable);
				conn.getSource().addOutgoingChannel(conn);
			}
			
			for (Channel c : visitable.getBroadcastInputs()) {
				c.setTarget(visitable);
				c.getSource().addOutgoingChannel(c);
			}

			// count the memory consumption
			this.memoryConsumerWeights += visitable.getMemoryConsumerWeight();
			for (Channel c : visitable.getInputs()) {
				if (c.getLocalStrategy().dams()) {
					this.memoryConsumerWeights++;
				}
				if (c.getTempMode() != TempMode.NONE) {
					this.memoryConsumerWeights++;
				}
			}
			for (Channel c : visitable.getBroadcastInputs()) {
				if (c.getLocalStrategy().dams()) {
					this.memoryConsumerWeights++;
				}
				if (c.getTempMode() != TempMode.NONE) {
					this.memoryConsumerWeights++;
				}
			}
			
			// pass the visitor to the iteraton's step function
			if (visitable instanceof IterationPlanNode) {
				// push the iteration node onto the stack
				final IterationPlanNode iterNode = (IterationPlanNode) visitable;
				this.stackOfIterationNodes.addLast(iterNode);
				
				// recurse
				((IterationPlanNode) visitable).acceptForStepFunction(this);
				
				// pop the iteration node from the stack
				this.stackOfIterationNodes.removeLast();
			}
			return true;
		}

		@Override
		public void postVisit(PlanNode visitable) {}
	}

	
	/**
	 * A visitor that traverses the graph and collects cascading binary unions into a single n-ary
	 * union operator. The exception is, when on of the union inputs is materialized, such as in the
	 * static-code-path-cache in iterations.
	 */
	private static final class BinaryUnionReplacer implements Visitor<PlanNode> {
		
		private final Set<PlanNode> seenBefore = new HashSet<PlanNode>();
		
		@Override
		public boolean preVisit(PlanNode visitable) {
			if (this.seenBefore.add(visitable)) {
				if (visitable instanceof IterationPlanNode) {
					((IterationPlanNode) visitable).acceptForStepFunction(this);
				}
				return true;
			} else {
				return false;
			}
		}

		@Override
		public void postVisit(PlanNode visitable) {
			
			if (visitable instanceof BinaryUnionPlanNode) {
				final BinaryUnionPlanNode unionNode = (BinaryUnionPlanNode) visitable;
				final Channel in1 = unionNode.getInput1();
				final Channel in2 = unionNode.getInput2();
			
				PlanNode newUnionNode;
				
				// if any input is cached, we keep this as a binary union and do not collapse it into a
				// n-ary union
//				if (in1.getTempMode().isCached() || in2.getTempMode().isCached()) {
//					// replace this node by an explicit operator
//					Channel cached, pipelined;
//					if (in1.getTempMode().isCached()) {
//						cached = in1;
//						pipelined = in2;
//					} else {
//						cached = in2;
//						pipelined = in1;
//					}
//					
//					newUnionNode = new DualInputPlanNode(unionNode.getOriginalOptimizerNode(), cached, pipelined,
//						DriverStrategy.UNION_WITH_CACHED);
//					newUnionNode.initProperties(unionNode.getGlobalProperties(), new LocalProperties());
//					
//					in1.setTarget(newUnionNode);
//					in2.setTarget(newUnionNode);
//				} else {
					// collect the union inputs to collapse this operator with 
					// its collapsed predecessors. check whether an input is materialized to prevent
					// collapsing
					List<Channel> inputs = new ArrayList<Channel>();
					collect(in1, inputs);
					collect(in2, inputs);
					
					newUnionNode = new NAryUnionPlanNode(unionNode.getOptimizerNode(), inputs, unionNode.getGlobalProperties());
					
					// adjust the input channels to have their target point to the new union node
					for (Channel c : inputs) {
						c.setTarget(newUnionNode);
					}
//				}
				
				unionNode.getOutgoingChannels().get(0).swapUnionNodes(newUnionNode);
			}
		}
		
		private void collect(Channel in, List<Channel> inputs) {
			if (in.getSource() instanceof NAryUnionPlanNode) {
				// sanity check
				if (in.getShipStrategy() != ShipStrategyType.FORWARD) {
					throw new CompilerException("Bug: Plan generation for Unions picked a ship strategy between binary plan operators.");
				}
				if (!(in.getLocalStrategy() == null || in.getLocalStrategy() == LocalStrategy.NONE)) {
					throw new CompilerException("Bug: Plan generation for Unions picked a local strategy between binary plan operators.");
				}
				
				inputs.addAll(((NAryUnionPlanNode) in.getSource()).getListOfInputs());
			} else {
				// is not a union node, so we take the channel directly
				inputs.add(in);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Miscellaneous
	// ------------------------------------------------------------------------
	
	private OptimizerPostPass getPostPassFromPlan(Plan program) {
		final String className =  program.getPostPassClassName();
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
		} catch (ClassNotFoundException cnfex) {
			throw new CompilerException("Cannot load Optimizer post-pass class '" + className + "'.", cnfex);
		} catch (ClassCastException ccex) {
			throw new CompilerException("Class '" + className + "' is not an optimizer post passer.", ccex);
		}
	}
}
