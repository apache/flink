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

package eu.stratosphere.pact.compiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.costs.FallbackCostEstimator;
import eu.stratosphere.pact.compiler.plan.CoGroupNode;
import eu.stratosphere.pact.compiler.plan.CrossNode;
import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.compiler.plan.DataSourceNode;
import eu.stratosphere.pact.compiler.plan.MapNode;
import eu.stratosphere.pact.compiler.plan.MatchNode;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.ReduceNode;
import eu.stratosphere.pact.compiler.plan.PactConnection.TempMode;

/**
 * The optimizer that takes the user specified pact plan and creates an optimized plan that contains
 * exact descriptions about how the physical execution will take place. It first translates the user
 * pact program into an internal optimizer representation and then chooses between different alternatives
 * for shipping strategies and local strategies.
 * <p>
 * The basic principle is taken from optimizer works in systems such as Volcano/Cascades and Selinger/System-R/DB2. The
 * optimizer walks from the sinks down, generating interesting properties, and ascends from the sources generating
 * alternative plans, pruning against the interesting properties.
 * <p>
 * The optimizer also assigns the memory to the individual tasks. This is currently done in a very simple fashion: All
 * sub-tasks that need memory (e.g. reduce or match) are given an equal share of memory.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class PactCompiler {
	// ------------------------------------------------------------------------
	// Constants
	// ------------------------------------------------------------------------

	/**
	 * Compiler hint key for the input channel's shipping strategy. This String is a key to the contract's stub
	 * parameters. The corresponding value tells the compiler which shipping strategy to use for the input channel.
	 * If the contract has two input channels, the shipping strategy is applied to both input channels.
	 */
	public static final String HINT_SHIP_STRATEGY = "INPUT_SHIP_STRATEGY";

	/**
	 * Compiler hint key for the <b>first</b> input channel's shipping strategy. This String is a key to
	 * the contract's stub parameters. The corresponding value tells the compiler which shipping strategy
	 * to use for the <b>first</b> input channel. Only applicable to contracts with two inputs.
	 */
	public static final String HINT_SHIP_STRATEGY_FIRST_INPUT = "INPUT_LEFT_SHIP_STRATEGY";

	/**
	 * Compiler hint key for the <b>second</b> input channel's shipping strategy. This String is a key to
	 * the contract's stub parameters. The corresponding value tells the compiler which shipping strategy
	 * to use for the <b>second</b> input channel. Only applicable to contracts with two inputs.
	 */
	public static final String HINT_SHIP_STRATEGY_SECOND_INPUT = "INPUT_RIGHT_SHIP_STRATEGY";

	/**
	 * Value for the shipping strategy compiler hint that enforces a <b>repartition</b> strategy on the
	 * input channel.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_REPARTITION = "SHIP_REPARTITION";

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
	 * Value for the shipping strategy compiler hint that enforces a <b>Forward</b> strategy on the
	 * input channel, i.e. no redistribution of any kind.
	 * 
	 * @see #HINT_SHIP_STRATEGY
	 * @see #HINT_SHIP_STRATEGY_FIRST_INPUT
	 * @see #HINT_SHIP_STRATEGY_SECOND_INPUT
	 */
	public static final String HINT_SHIP_STRATEGY_FORWARD = "SHIP_FORWARD";

	/**
	 * Compiler hint key for the contract's local strategy. This String is a key to the contract's stub
	 * parameters. The corresponding value tells the compiler which local strategy to use to process the
	 * data inside one partition.
	 * <p>
	 * This hint is ignored by contracts that do not have a local strategy (such as <i>Map</i>), or by contracts that
	 * have no choice in their local strategy (such as <i>Cross</i>).
	 */
	public static final String HINT_LOCAL_STRATEGY = "LOCAL_STRATEGY";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort based</b> local strategy.
	 * For example, a <i>Reduce</i> contract will sort the data to group it, a <i>Match</i> contract
	 * will use a sort-merge strategy to find pairs of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT = "LOCAL_STRATEGY_SORT";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>hash based</b> local strategy.
	 * For example, a <i>Match</i> contract will use a hybrid-hash-join strategy to find pairs of
	 * matching keys. The <b>first</b> input will be used to build the hash table, the second input will be
	 * used to probe the table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST = "LOCAL_STRATEGY_HASH_BUILD_FIRST";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>hash based</b> local strategy.
	 * For example, a <i>Match</i> contract will use a hybrid-hash-join strategy to find pairs of
	 * matching keys. The <b>second</b> input will be used to build the hash table, the first input will be
	 * used to probe the table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND = "LOCAL_STRATEGY_HASH_BUILD_SECOND";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>in-memory hash based</b> local strategy.
	 * For example, a <i>Match</i> contract will use a hash-join strategy where the hash-table is kept entirely
	 * in main memory. The <b>first</b> input will be used to build the hash table, the second input will be
	 * used to probe the table.
	 * <p>
	 * NOTE: Tasks with this this local strategy may fail, if there is not enough main memory for the hash-table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_FIRST = "LOCAL_STRATEGY_INMEM_HASH_BUILD_FIRST";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>in-memory hash based</b> local strategy.
	 * For example, a <i>Match</i> contract will use a hash-join strategy where the hash-table is kept entirely
	 * in main memory. The <b>second</b> input will be used to build the hash table, the first input will be
	 * used to probe the table.
	 * <p>
	 * NOTE: Tasks with this this local strategy may fail, if there is not enough main memory for the hash-table.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_SECOND = "LOCAL_STRATEGY_INMEM_HASH_BUILD_SECOND";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> contract will process the data of the <b>first</b> input in the outer-loop of the nested loops.
	 * Hence, the data of the first input will be is streamed though, while the data of the second input is stored on
	 * disk
	 * and repeatedly read.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST = "LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> contract will process the data of the <b>second</b> input in the outer-loop of the nested loops.
	 * Hence, the data of the second input will be is streamed though, while the data of the first input is stored on
	 * disk
	 * and repeatedly read.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND = "LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> contract will process the data of the <b>first</b> input in the outer-loop of the nested loops.
	 * Further more, the first input, being the outer side, will be processed in blocks, and for each block, the second
	 * input,
	 * being the inner side, will read repeatedly from disk.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST = "LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST";

	/**
	 * Value for the local strategy compiler hint that chooses the outer side of the <b>nested-loop</b> local strategy.
	 * A <i>Cross</i> contract will process the data of the <b>second</b> input in the outer-loop of the nested loops.
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
	 * The type of instance that the PACT vertices are scheduled on.
	 */
	private final InstanceType pactInstanceType;

	/**
	 * The default degree of parallelism for jobs compiled by this compiler.
	 */
	private final int defaultDegreeOfParallelism;

	/**
	 * The default number of subtasks that should share an instance.
	 */
	private final int defaultIntraNodeParallelism;

	/**
	 * The amount of memory usable for PACT code per instance (in MB).
	 */
	private final int memoryPerInstance;

	// ------------------------------------------------------------------------
	// Constructor & Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust strategy to fulfill the PACTs. The
	 * compiler also uses conservative default estimates for the operator costs, since
	 * it has no access to another cost estimator.
	 */
	public PactCompiler() {
		this(null, new FallbackCostEstimator());
	}

	/**
	 * Creates a new compiler instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the compiler can make better choices for the execution strategies.
	 * as if no filesystem was given. The compiler uses conservative default estimates for the operator costs, since
	 * it has no access to another cost estimator.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 */
	public PactCompiler(DataStatistics stats) {
		this(stats, new FallbackCostEstimator());
	}

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust strategy to fulfill the PACTs. It uses
	 * however the given cost estimator to compute the costs of the individual operations.
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
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 * @param estimator
	 *        The <tt>CostEstimator</tt> to use to cost the individual operations.
	 */
	public PactCompiler(DataStatistics stats, CostEstimator estimator)
	{
		this.statistics = stats;
		this.costEstimator = estimator;

		Configuration config = GlobalConfiguration.getConfiguration();
		
		// get the instance type to schedule pact tasks on		
		String instanceDescr = config.getString(PactConfigConstants.DEFAULT_INSTANCE_TYPE_KEY,
			PactConfigConstants.DEFAULT_INSTANCE_TYPE_DESCRIPTION);
		InstanceType type = null;
		try {
			type = InstanceTypeFactory.constructFromDescription(instanceDescr);
		}
		catch (IllegalArgumentException iaex) {
			LOG.error("Invalid description of standard instance type in PACT configuration: " + instanceDescr + 
				". Using default instance type " + PactConfigConstants.DEFAULT_INSTANCE_TYPE_DESCRIPTION + ".", iaex);
			type = InstanceTypeFactory.constructFromDescription(PactConfigConstants.DEFAULT_INSTANCE_TYPE_DESCRIPTION);
		}
		this.pactInstanceType = type;

		// determine the default parallelization degree
		int defaultParallelizationDegree = config.getInteger(PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY,
			PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE);
		if (defaultParallelizationDegree < 1) {
			LOG.error("Invalid default degree of parallelism: " + defaultParallelizationDegree + 
				". Using default degree of " + PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE + ".");
			defaultParallelizationDegree = PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE;
		}
		this.defaultDegreeOfParallelism = defaultParallelizationDegree;

		// determine the default intra-node parallelism
		int defaultInNodePar = config.getInteger(PactConfigConstants.DEFAULT_PARALLELIZATION_INTRA_NODE_DEGREE_KEY,
			PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE);
		if (defaultInNodePar < 1) {
			LOG.error("Invalid default degree of intra-node parallelism: " + defaultParallelizationDegree + 
				". Using default degree of " + PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE + ".");
			defaultInNodePar = PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE;
		}
		this.defaultIntraNodeParallelism = defaultInNodePar;

		// get the amount of memory usable per instance
		this.memoryPerInstance = pactInstanceType.getMemorySize();
	}

	// ------------------------------------------------------------------------
	// Compilation
	// ------------------------------------------------------------------------

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
	 * @param pactPlan
	 *        The PACT plan to be translated.
	 * @return The optimized plan.
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	public OptimizedPlan compile(Plan pactPlan) throws CompilerException {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Beginning compilation of PACT program '" + pactPlan.getJobName() + '\'');
		}

		Configuration config = GlobalConfiguration.getConfiguration();

		// determine the number of machines to use
		int maxMachinesConfig = config.getInteger(PactConfigConstants.MAXIMUM_NUMBER_MACHINES_KEY,
			PactConfigConstants.DEFAULT_MAX_NUMBER_MACHINES);

		int maxMachines = pactPlan.getMaxNumberMachines();

		if (maxMachines < 1) {
			maxMachines = maxMachinesConfig;
		} else if (maxMachinesConfig >= 1) {
			// check if the program requested more than the global config allowed
			if (maxMachines > maxMachinesConfig && LOG.isWarnEnabled()) {
				LOG.warn("Maximal number of machines specified in PACT program (" + maxMachines
					+ ") exceeds the maximum number in the global configuration (" + maxMachinesConfig
					+ "). Using the value given in the global configuration.");
			}

			maxMachines = Math.min(maxMachines, maxMachinesConfig);
		}

		// log the output
		if (LOG.isDebugEnabled()) {
			LOG.debug("Using a default degree of parallelism of " + this.defaultDegreeOfParallelism
				+ ", a default intra-node parallelism of " + this.defaultIntraNodeParallelism + '.');
			if (maxMachines > 0) {
				LOG.debug("The execution is limited to a maximum number of " + maxMachines + " machines.");
			}

		}

		// the first step in the compilation is to create the optimizer plan representation
		// this step does the following:
		// 1) It creates an optimizer plan node for each pact
		// 2) It connects them via channels
		// 3) It looks for hints about local strategies and channel types and
		// sets the types and strategies accordingly
		// 4) It makes estimates about the data volume of the data sources and
		// propagates those estimates through the plan

		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(maxMachines, true);
		pactPlan.accept(graphCreator);
		OptimizedPlan plan = new OptimizedPlan(graphCreator.sources, graphCreator.sinks,
			graphCreator.con2node.values(), pactPlan.getJobName());

		// Now that the previous step is done, the next step is to traverse the graph again for the two
		// steps that cannot directly be performed during the plan enumeration, because we are dealing with DAGs
		// rather than a trees. That requires us to deviate at some points from the classical DB optimizer algorithms.
		// 
		// 1) propagate the interesting properties top-down through the graph
		// 2) Track information about nodes with multiple outputs that are later on reconnected in a node with
		// multiple inputs.
		InterestingPropertyAndBranchesVisitor propsVisitor = new InterestingPropertyAndBranchesVisitor(costEstimator);
		plan.accept(propsVisitor);

		// the final step is not to generate the actual plan alternatives
		// currently, we support only one data sink
		if (plan.getDataSinks().size() != 1) {
			throw new CompilerException("In the current version, plans must have exactly one data sink.");
		}

		List<DataSinkNode> bestPlan = plan.getDataSinks().iterator().next().getAlternativePlans(costEstimator);
		if (bestPlan.size() != 1) {
			throw new CompilerException("Error in compiler: more than one best plan was created!");
		}

		// finalize the plan
		plan = new PlanFinalizer().createFinalPlan(bestPlan, pactPlan.getJobName(), memoryPerInstance);
		plan.setInstanceTypeName(this.pactInstanceType.getIdentifier());

		// insert temporary dams, as they may be necessary in non-tree graphs to prevent deadlocks
		insertTempConnection(plan);

		return plan;
	}

	/**
	 * This function performs only the first step to the compilation process - the creation of the optimizer
	 * representation of the plan. No estimations or enumerations of alternatives are done here.
	 * 
	 * @param pactPlan
	 *        The plan to generate the optimizer representation for.
	 * @return The optimizer representation of the plan.
	 */
	public OptimizedPlan createPreOptimizedPlan(Plan pactPlan) {
		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(-1, false);
		pactPlan.accept(graphCreator);
		return new OptimizedPlan(graphCreator.sources, graphCreator.sinks, graphCreator.con2node.values(), pactPlan
			.getJobName());
	}

	/**
	 * This utility class performs the translation from the user specified PACT job to the optimizer plan.
	 * It works as a visitor that walks the user's job in a depth-first fashion. During the descend, it creates
	 * an optimizer node for each pact, respectively data source or -sink. During the ascend, it connects
	 * the nodes to the full graph.
	 * <p>
	 * This translator relies on the <code>setInputs</code> method in the nodes. As that method implements the size
	 * estimation and the awareness for optimizer hints, the sizes will be properly estimated and the translated plan
	 * already respects all optimizer hints.
	 */
	private final class GraphCreatingVisitor implements Visitor<Contract> {
		private final Map<Contract, OptimizerNode> con2node; // map from the contract objects to their

		// corresponding optimizer nodes

		private final List<DataSourceNode> sources; // all data source nodes in the optimizer plan

		private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

		private final int maxMachines; // the maximum number of machines to use

		private int id; // the incrementing id for the nodes.

		private final boolean computeEstimates; // flag indicating whether to compute additional info

		/**
		 * Creates a new node creating visitor.
		 */
		private GraphCreatingVisitor(int maxMachines, boolean computeEstimates) {
			this.con2node = new HashMap<Contract, OptimizerNode>();
			this.sources = new ArrayList<DataSourceNode>(4);
			this.sinks = new ArrayList<DataSinkNode>(2);

			this.maxMachines = maxMachines;

			this.id = 1;

			this.computeEstimates = computeEstimates;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public boolean preVisit(Contract c) {
			// check if we have been here before
			if (con2node.containsKey(c)) {
				return false;
			}

			OptimizerNode n = null;

			// create a node for the pact (or sink or source) if we have not been here before
			if (c instanceof DataSinkContract<?, ?>) {
				DataSinkNode dsn = new DataSinkNode((DataSinkContract<?, ?>) c);
				sinks.add(dsn);
				n = dsn;
			} else if (c instanceof DataSourceContract<?, ?>) {
				DataSourceNode dsn = new DataSourceNode((DataSourceContract<?, ?>) c);
				sources.add(dsn);
				n = dsn;
			} else if (c instanceof MapContract<?, ?, ?, ?>) {
				n = new MapNode((MapContract<?, ?, ?, ?>) c);
			} else if (c instanceof ReduceContract<?, ?, ?, ?>) {
				n = new ReduceNode((ReduceContract<?, ?, ?, ?>) c);
			} else if (c instanceof MatchContract<?, ?, ?, ?, ?>) {
				n = new MatchNode((MatchContract<?, ?, ?, ?, ?>) c);
			} else if (c instanceof CoGroupContract<?, ?, ?, ?, ?>) {
				n = new CoGroupNode((CoGroupContract<?, ?, ?, ?, ?>) c);
			} else if (c instanceof CrossContract<?, ?, ?, ?, ?, ?>) {
				n = new CrossNode((CrossContract<?, ?, ?, ?, ?, ?>) c);
			} else {
				throw new IllegalArgumentException("Unknown contract type.");
			}

			con2node.put(c, n);

			// set the degree of parallelism
			int par = c.getDegreeOfParallelism();
			par = par >= 1 ? par : PactCompiler.this.defaultDegreeOfParallelism;

			// set the parallelism only if it has not been set before
			if (n.getDegreeOfParallelism() < 1) {
				n.setDegreeOfParallelism(par);
			}

			// check if we need to set the instance sharing accordingly such that
			// the maximum number of machines is not exceeded
			int tasksPerInstance = 1;
			if (maxMachines > 0) {
				int p = n.getDegreeOfParallelism();
				int mpi = p / maxMachines;
				mpi += p % maxMachines == 0 ? 0 : 1;

				tasksPerInstance = Math.max(mpi, PactCompiler.this.defaultIntraNodeParallelism);
			}

			// we group together n tasks per machine, depending on config and the above computed
			// value required to obey the maximum number of machines
			n.setInstancesPerMachine(tasksPerInstance);

			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public void postVisit(Contract c) {
			OptimizerNode n = con2node.get(c);
			n.SetId(id++);

			// first connect to the predecessors
			n.setInputs(con2node);

			// now compute the output estimates
			if (computeEstimates) {
				n.computeOutputEstimates(statistics);
			}
		}

	};

	/**
	 * Visitor that computes the interesting properties for each node in the plan. On its recursive
	 * depth-first descend, it propagates all interesting properties top-down. On its re-ascend,
	 * it computes auxiliary maps that are needed to support plans that are not a minimally connected
	 * DAG (Such plans are not trees, but at least one node feeds its output into more than one other
	 * node).
	 */
	private static final class InterestingPropertyAndBranchesVisitor implements Visitor<OptimizerNode> {

		private CostEstimator estimator; // the cost estimator used to compute the

		// maximal costs for an interesting property

		/**
		 * Creates a new visitor that computes the interesting properties for all nodes in the plan.
		 * It uses the given cost estimator used to compute the maximal costs for an interesting property.
		 * 
		 * @param estimator
		 *        The cost estimator to estimate the maximal costs for interesting properties.
		 */
		InterestingPropertyAndBranchesVisitor(CostEstimator estimator) {
			this.estimator = estimator;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public boolean preVisit(OptimizerNode node) {
			// The interesting properties must be computed on the descend. In case a node has multiple outputs,
			// that computation must happen during the last descend.

			if (node.haveAllOutputConnectionInterestingProperties() && node.getInterestingProperties() == null) {
				node.computeInterestingProperties();
				node.computeInterestingPropertiesForInputs(estimator);
			}

			// make sure we descend in any case (even if it causes redundant descends), because the branch propagation
			// during the post visit needs to happen during the first re-ascend
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public void postVisit(OptimizerNode node) {
			node.computeUnclosedBranchStack();
		}
	};

	/**
	 * Utility class that traverses a plan to collect all nodes and add them to the OptimizedPlan.
	 * Besides collecting all nodes, this traversal assigns the memory to the nodes.
	 */
	private static final class PlanFinalizer implements Visitor<OptimizerNode> {
		private final Set<OptimizerNode> allNodes; // a set of all nodes in the optimizer plan

		private final List<DataSourceNode> sources; // all data source nodes in the optimizer plan

		private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

		private int memoryConsumers; // a counter of all memory consumers

		/**
		 * Creates a new plan finalizer.
		 */
		private PlanFinalizer() {
			this.allNodes = new HashSet<OptimizerNode>();
			this.sources = new ArrayList<DataSourceNode>();
			this.sinks = new ArrayList<DataSinkNode>();
		}

		private OptimizedPlan createFinalPlan(List<DataSinkNode> sinks, String jobName, int memoryPerInstance) {
			// traverse the graph
			for (DataSinkNode node : sinks) {
				node.accept(this);
			}

			// assign the memory to each node
			if (memoryConsumers > 0) {
				int memoryPerTask = memoryPerInstance / memoryConsumers;
				for (OptimizerNode node : this.allNodes) {
					if (node.isMemoryConsumer()) {
						node.setMemoryPerTask(memoryPerTask);
					}
				}
			}

			return new OptimizedPlan(this.sources, this.sinks, this.allNodes, jobName);
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public boolean preVisit(OptimizerNode visitable) {
			// if we come here again, prevent a further descend
			if (!allNodes.add(visitable)) {
				return false;
			}

			for (PactConnection c : visitable.getIncomingConnections()) {
				c.getSourcePact().addOutgoingConnection(c);
			}

			if (visitable instanceof DataSinkNode) {
				sinks.add((DataSinkNode) visitable);
			} else if (visitable instanceof DataSourceNode) {
				sources.add((DataSourceNode) visitable);
			}

			// count the memory consumption
			if (visitable.isMemoryConsumer()) {
				memoryConsumers += visitable.getInstancesPerMachine();
			}

			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see
		 * eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
		 */
		@Override
		public void postVisit(OptimizerNode visitable) {
			// do nothing
		}
	}

	// ------------------------------------------------------------------------
	// Inserting dams to break pipeline deadlocks in non-tree graphs
	// ------------------------------------------------------------------------

	/**
	 * Inserts temping connections where it is necessary.
	 */
	// TODO: this should be integrated into the optimization and not done as a postprocessing
	private void insertTempConnection(OptimizedPlan plan) {

		for (OptimizerNode node : plan.getAllNodes()) {
			for (PactConnection inConn : node.getIncomingConnections()) {
				// check if inConn is a blocked connection
				if (isBlockedConnection(inConn)) {
					// check if inConn needs to be temped
					if (needsToBeTemped(inConn)) {
						// temp inConn
						// TODO: decide smarter where to put temp (sender/receiver, connection)
						inConn.setTempMode(TempMode.TEMP_RECEIVER_SIDE);
					}
				}
			}
		}
	}

	/**
	 * Checks whether a connection is blocked (waiting for other connection to be fully read).
	 * Blocked connections are:
	 * - Connection to Match PACTs that probe a HashTable
	 * - Connection to Cross PACTs that are the outer loop of the nested-loop strategy
	 * Connections that are explicitly temping are never blocked!
	 * 
	 * @param conn
	 *        Connection that is checked for being a blocked connection.
	 * @return True if the connection is blocked, False otherwise.
	 */
	private boolean isBlockedConnection(PactConnection conn) {
		if (conn.getTempMode() != TempMode.NONE) {
			return false;
		}

		switch (conn.getTargetPact().getPactType()) {
		case Match:
			// test if hash strategy is used
			int inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
			switch (conn.getTargetPact().getLocalStrategy()) {
			case HYBRIDHASH_FIRST:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case HYBRIDHASH_SECOND:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case MMHASH_FIRST:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case MMHASH_SECOND:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			default:
				return false;
			}
		case Cross:
			inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
			switch (conn.getTargetPact().getLocalStrategy()) {
			case NESTEDLOOP_BLOCKED_OUTER_SECOND:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case NESTEDLOOP_STREAMED_OUTER_SECOND:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case NESTEDLOOP_STREAMED_OUTER_FIRST:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			default:
				return false;
			}
		default:
			return false;
		}
	}

	/**
	 * Checks whether a connection is temping (consuming all records before processing).
	 * Temping connections are:
	 * - Connections to Reduce PACTs
	 * - Connections to CoGroup PACTs
	 * - Connections to Match PACTs with MergeSort LocalStrategy
	 * - Connections to Match PACTs that build a HashTable
	 * - Connections to Cross PACTs that are read into the resetable iterator
	 * - Connections that are explicitly tempings
	 * 
	 * @param conn
	 *        Connection that is checked for temping
	 * @return True if the connection is temping, False otherwise
	 */
	private boolean isTempingConnection(PactConnection conn) {
		if (conn.getTempMode() != TempMode.NONE) {
			return true;
		}

		switch (conn.getTargetPact().getPactType()) {
		case Reduce:
			return true;
		case Match:
			int inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
			switch (conn.getTargetPact().getLocalStrategy()) {
			case SORT:
				return true;
			case HYBRIDHASH_FIRST:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case HYBRIDHASH_SECOND:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case MMHASH_FIRST:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case MMHASH_SECOND:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			default:
				return false;
			}
		case Cross:
			inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
			switch (conn.getTargetPact().getLocalStrategy()) {
			case NESTEDLOOP_BLOCKED_OUTER_SECOND:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case NESTEDLOOP_STREAMED_OUTER_SECOND:
				if (inConnIdx == 0)
					return true;
				else
					return false;
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			case NESTEDLOOP_STREAMED_OUTER_FIRST:
				if (inConnIdx == 1)
					return true;
				else
					return false;
			}
		case Cogroup:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Checks whether a blocked connection needs to be temped.
	 * A blocked connection is a dam since it has to wait until the input of the other connection is fully read
	 * (probe-side for hash strategies, streaming-side for nested-loop strategy).
	 * The dam becomes critical if the blocking connection is fed by a node, that has multiple outputs.
	 * Since the other targets of the multi-output-node have to wait for the blocked connection to flow.
	 * This might cause following problems:
	 * 1) Deadlocks
	 * 2) Performance decrease due to time to wait for blocking connections
	 * The problem vanishes if a temping node (a node that consumes all tuples before processing) lies on the path
	 * from the multi-output-node to the blocked connection.
	 * This method checks if a blocked connection is fed by a multi-output-node and whether a temping node exists on the
	 * path.
	 * If the blocked connection is not fed by a multi-output node it does not need be be temped.
	 * Otherwise, it needs to be temped if there are no temping nodes on the path.
	 * 
	 * @param conn
	 *        Blocked connection that should be checked for temping
	 * @return True if the blocked connection must be temped, False otherwise.
	 */
	private boolean needsToBeTemped(PactConnection conn) {
		if (isTempingConnection(conn)) {
			return false;
		} else {
			if (conn.getSourcePact().getOutgoingConnections().size() > 1) {
				return true;
			} else {
				for (PactConnection inConn : conn.getSourcePact().getIncomingConnections()) {
					if (needsToBeTemped(inConn) == true) {
						return true;
					}
				}
				return false;
			}
		}
	}

}
