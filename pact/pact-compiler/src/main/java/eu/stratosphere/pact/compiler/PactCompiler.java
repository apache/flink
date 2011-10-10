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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.ipc.RPC;
import eu.stratosphere.nephele.net.NetUtils;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.GenericDataSinkContract;
import eu.stratosphere.pact.common.contract.GenericDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.costs.FixedSizeClusterCostEstimator;
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
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.SinkJoiner;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.compiler.plan.PactConnection.TempMode;
import eu.stratosphere.pact.runtime.task.HistogramTask;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

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
	 * For example, a <i>Reduce</i> contract will sort the data to group it.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT = "LOCAL_STRATEGY_SORT";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort based</b> local strategy.
	 * During sorting a combine method is repeatedly applied to reduce the data volume.
	 * For example, a <i>Reduce</i> contract will sort the data to group it.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_COMBINING_SORT = "LOCAL_STRATEGY_COMBINING_SORT";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy on both
	 * inputs with subsequent merging of inputs. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> contract will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE = "LOCAL_STRATEGY_SORT_BOTH_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy.
	 * The the first input is sorted, the second input is assumed to be sorted. After sorting both inputs are merged. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> contract will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE = "LOCAL_STRATEGY_SORT_FIRST_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort merge based</b> local strategy.
	 * The the second input is sorted, the first input is assumed to be sorted. After sorting both inputs are merged. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> contract will use a sort-merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE = "LOCAL_STRATEGY_SORT_SECOND_MERGE";
	
	/**
	 * Value for the local strategy compiler hint that enforces a <b>merge based</b> local strategy.
	 * Both inputs are assumed to be sorted and are merged. 
	 * For example, a <i>Match</i> or <i>CoGroup</i> contract will use a merge strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_MERGE = "LOCAL_STRATEGY_MERGE";

	/**
	 * Value for the local strategy compiler hint that enforces a <b>sort based</b> local strategy with self crossing.
	 * For example, a <i>Match</i> contract whose inputs are identical will use this strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SORT_SELF_NESTEDLOOP = "LOCAL_STRATEGY_SORT_SELF_NESTEDLOOP";
	
	/**
	 * Value for the local strategy compiler hint that enforces a self crossing local strategy.
	 * For example, a <i>Match</i> contract whose inputs are identical and already grouped will use this strategy to find pairs 
	 * of matching keys.
	 * 
	 * @see #HINT_LOCAL_STRATEGY
	 */
	public static final String HINT_LOCAL_STRATEGY_SELF_NESTEDLOOP = "LOCAL_STRATEGY_SELF_NESTEDLOOP";

	
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

	public static final int DEFAULT_TEMP_TASK_MEMORY = 4; // the amount of memory for TempTasks in MB
	
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
	 * The connection used to connect to the job-manager.
	 */
	private final InetSocketAddress jobManagerAddress;

	/**
	 * The maximum number of machines (instances) to use, per the configuration.
	 */
	private final int maxMachines;

	/**
	 * The default degree of parallelism for jobs compiled by this compiler.
	 */
	private final int defaultDegreeOfParallelism;

	/**
	 * The default number of subtasks that should share an instance.
	 */
	private final int defaultIntraNodeParallelism;

	// ------------------------------------------------------------------------
	// Constructor & Setup
	// ------------------------------------------------------------------------

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust strategy to fulfill the PACTs. The
	 * compiler also uses conservative default estimates for the operator costs, since
	 * it has no access to another cost estimator.
	 * <p>
	 * The address of the job manager (to obtain system characteristics) is determined via the global configuration.
	 */
	public PactCompiler() {
		this(null, new FixedSizeClusterCostEstimator());
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
		this(stats, new FixedSizeClusterCostEstimator());
	}

	/**
	 * Creates a new compiler instance. The compiler has no access to statistics about the
	 * inputs and can hence not determine any properties. It will perform all optimization with
	 * unknown sizes and default to the most robust strategy to fulfill the PACTs. It uses
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
		this(stats, estimator, null);
	}

	/**
	 * Creates a new compiler instance that uses the statistics object to determine properties about the input.
	 * Given those statistics, the compiler can make better choices for the execution strategies.
	 * as if no filesystem was given. It uses the given cost estimator to compute the costs of the individual
	 * operations.
	 * <p>
	 * The given socket-address is used to connect to the job manager to obtain system characteristics, like available
	 * memory. If that parameter is null, then the address is obtained from the global configuration.
	 * 
	 * @param stats
	 *        The statistics to be used to determine the input properties.
	 * @param estimator
	 *        The <tt>CostEstimator</tt> to use to cost the individual operations.
	 * @param jobManagerConnection
	 *        The address of the job manager that is queried for system characteristics.
	 */
	public PactCompiler(DataStatistics stats, CostEstimator estimator, InetSocketAddress jobManagerConnection) {
		this.statistics = stats;
		this.costEstimator = estimator;

		Configuration config = GlobalConfiguration.getConfiguration();

		// determine the maximum number of instances to use
		this.maxMachines = config.getInteger(PactConfigConstants.MAXIMUM_NUMBER_MACHINES_KEY,
			PactConfigConstants.DEFAULT_MAX_NUMBER_MACHINES);

		// determine the default parallelization degree
		this.defaultDegreeOfParallelism = config.getInteger(PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE_KEY,
			PactConfigConstants.DEFAULT_PARALLELIZATION_DEGREE);

		// determine the default intra-node parallelism
		int defaultInNodePar = config.getInteger(PactConfigConstants.DEFAULT_PARALLELIZATION_INTRA_NODE_DEGREE_KEY,
			PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE);
		if (defaultInNodePar < 1) {
			LOG.error("Invalid default degree of intra-node parallelism: " + defaultInNodePar +
				". Using default degree of " + PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE + ".");
			defaultInNodePar = PactConfigConstants.DEFAULT_INTRA_NODE_PARALLELIZATION_DEGREE;
		}
		this.defaultIntraNodeParallelism = defaultInNodePar;

		// assign the connection to the job-manager
		if (jobManagerConnection != null) {
			this.jobManagerAddress = jobManagerConnection;
		} else {
			final String address = config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
			if (address == null) {
				throw new CompilerException(
					"Cannot find address to job manager's RPC service in the global configuration.");
			}

			final int port = GlobalConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);
			if (port < 0) {
				throw new CompilerException(
					"Cannot find port to job manager's RPC service in the global configuration.");
			}

			this.jobManagerAddress = new InetSocketAddress(address, port);
		}
	}

	// ------------------------------------------------------------------------
	//                               Compilation
	// ------------------------------------------------------------------------

	/**
	 * Translates the given pact plan in to an OptimizedPlan, where all nodes have their local strategy assigned
	 * and all channels have a shipping strategy assigned. The compiler connects to the job manager to obtain information
	 * about the available instances and their memory and then chooses an instance type to schedule the execution on.
	 * <p>
	 * The compilation process itself goes through several phases:
	 * <ol>
	 * <li>Create <tt>OptimizerNode</tt> representations of the PACTs, assign parallelism and compute size estimates.</li>
	 * <li>Compute interesting properties and auxiliary structures.</li>
	 * <li>Enumerate plan alternatives. This cannot be done in the same step as the interesting property computation (as
	 * opposed to the Database approaches), because we support plans that are not trees.</li>
	 * </ol>
	 * 
	 * @param pactPlan The PACT plan to be translated.
	 * @return The optimized plan.
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	public OptimizedPlan compile(Plan pactPlan) throws CompilerException
	{
		// -------------------- try to get the connection to the job manager ----------------------
		// --------------------------to obtain instance information --------------------------------

		if (LOG.isDebugEnabled()) {
			LOG.debug("Connecting compiler to JobManager to dertermine instance information.");
		}
		
		// create the connection in a separate thread, such that this thread
		// can abort, if an unsuccessful connection occurs.
		Map<InstanceType, InstanceTypeDescription> instances = null;
		
		JobManagerConnector jmc = new JobManagerConnector(this.jobManagerAddress);
		Thread connectorThread = new Thread(jmc, "Compiler - JobManager connector.");
		connectorThread.setDaemon(true);
		connectorThread.start();

		// connect and get the result
		try {
			jmc.waitForCompletion();
			instances = jmc.instances;
			if (instances == null) {
				throw new NullPointerException("Returned instance map is <null>");
			}
		}
		catch (Throwable t) {
			throw new CompilerException("Available instances could not be determined from job manager: " + 
				t.getMessage(), t);
		}

		// determine which type to run on
		InstanceTypeDescription type = getType(instances);
		
		return compile(pactPlan, type);
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
	 * @param pactPlan The PACT plan to be translated.
	 * @param type The instance type to schedule the execution on. Used also to determine the amount of memory
	 *             available to the tasks.
	 * @return The optimized plan.
	 * @throws CompilerException
	 *         Thrown, if the plan is invalid or the optimizer encountered an inconsistent
	 *         situation during the compilation process.
	 */
	public OptimizedPlan compile(Plan pactPlan, InstanceTypeDescription type) throws CompilerException
	{
		if (LOG.isDebugEnabled()) {
			LOG.debug("Beginning compilation of PACT program '" + pactPlan.getJobName() + '\'');
		}
		
		String instanceName = type.getInstanceType().getIdentifier();
		long memoryPerInstance = type.getHardwareDescription().getSizeOfFreeMemory();
		int memoryMegabytes = (int) (memoryPerInstance >>> 20);
		int numInstances = type.getMaximumNumberOfAvailableInstances();
		
		// determine the maximum number of machines to use
		int maxMachinesJob = pactPlan.getMaxNumberMachines();

		if (maxMachinesJob < 1) {
			maxMachinesJob = this.maxMachines;
		} else if (this.maxMachines >= 1) {
			// check if the program requested more than the global config allowed
			if (maxMachinesJob > this.maxMachines && LOG.isWarnEnabled()) {
				LOG.warn("Maximal number of machines specified in PACT program (" + maxMachinesJob
					+ ") exceeds the maximum number in the global configuration (" + this.maxMachines
					+ "). Using the value given in the global configuration.");
			}

			maxMachinesJob = Math.min(maxMachinesJob, this.maxMachines);
		}

		// adjust the maximum number of machines the the number of available instances
		if (maxMachinesJob < 1) {
			maxMachinesJob = numInstances;
		} else if (maxMachinesJob > numInstances) {
			maxMachinesJob = numInstances;
			if (LOG.isInfoEnabled()) {
				LOG.info("Maximal number of machines decreased to " + maxMachinesJob +
					" because no more instances are available.");
			}
		}

		// set the default degree of parallelism
		int defaultParallelism = this.defaultDegreeOfParallelism;
		if (defaultParallelism < 1) {
			defaultParallelism = maxMachinesJob * defaultIntraNodeParallelism;
		} else if (defaultParallelism > maxMachinesJob * defaultIntraNodeParallelism) {
			int oldParallelism = defaultParallelism;
			defaultParallelism = maxMachinesJob * defaultIntraNodeParallelism;

			if (LOG.isInfoEnabled()) {
				LOG.info("Decreasing default degree of parallelism from " + oldParallelism +
					" to " + defaultParallelism + " to fit a maximum number of " + maxMachinesJob +
					" instances with a intra-parallelism of " + defaultIntraNodeParallelism);
			}
		}

		// log the output
		if (LOG.isDebugEnabled()) {
			LOG.debug("Using a default degree of parallelism of " + defaultParallelism +
				", a default intra-node parallelism of " + this.defaultIntraNodeParallelism + '.');
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

		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(this.statistics, maxMachinesJob,
			defaultParallelism, this.defaultIntraNodeParallelism, true);
		pactPlan.accept(graphCreator);

		// if we have a plan with multiple data sinks, add logical optimizer nodes that have two data-sinks as children
		// each until we have only a single root node. This allows to transparently deal with the nodes with
		// multiple outputs
		OptimizerNode rootNode = null;

		if (graphCreator.sinks.size() == 1) {
			rootNode = graphCreator.sinks.get(0);
		} else if (graphCreator.sinks.size() > 1) {
			Iterator<DataSinkNode> iter = graphCreator.sinks.iterator();
			rootNode = iter.next();
			int id = graphCreator.getId();

			while (iter.hasNext()) {
				rootNode = new SinkJoiner(rootNode, iter.next());
				rootNode.SetId(id++);
			}
		} else {
			throw new CompilerException("The plan encountered when generating alternatives has no sinks.");
		}

		// Now that the previous step is done, the next step is to traverse the graph again for the two
		// steps that cannot directly be performed during the plan enumeration, because we are dealing with DAGs
		// rather than a trees. That requires us to deviate at some points from the classical DB optimizer algorithms.
		//
		// 1) propagate the interesting properties top-down through the graph
		// 2) Track information about nodes with multiple outputs that are later on reconnected in a node with
		// multiple inputs.
		InterestingPropertyAndBranchesVisitor propsVisitor = new InterestingPropertyAndBranchesVisitor(
			this.costEstimator);
		rootNode.accept(propsVisitor);

		// the final step is now to generate the actual plan alternatives
		List<? extends OptimizerNode> bestPlan = rootNode.getAlternativePlans(this.costEstimator);

		if (bestPlan.size() != 1) {
			throw new CompilerException("Error in compiler: more than one best plan was created!");
		}

		// check if the best plan's root is a data sink (single sink plan)
		// if so, directly take it. if it is a sink joiner node, get its contained sinks
		OptimizerNode bestPlanRoot = bestPlan.get(0);
		List<DataSinkNode> bestPlanSinks = new ArrayList<DataSinkNode>(4);

		if (bestPlanRoot instanceof DataSinkNode) {
			bestPlanSinks.add((DataSinkNode) bestPlanRoot);
		} else if (bestPlanRoot instanceof SinkJoiner) {
			((SinkJoiner) bestPlanRoot).getDataSinks(bestPlanSinks);
		}
		
		// connect nodes bidirectional
		new NodeConnector().connectNodes(bestPlanSinks);

		// insert temporary dams, as they may be necessary in non-tree graphs to prevent deadlocks
		Configuration config = GlobalConfiguration.getConfiguration();
		new DeadlockResolver(config.getBoolean("channel.network.allowSpilling",true)).resolveDeadlocks(bestPlanSinks);

		// finalize the plan
		OptimizedPlan plan = new PlanFinalizer().createFinalPlan(bestPlanSinks, pactPlan.getJobName(), memoryMegabytes);
		plan.setInstanceTypeName(instanceName);
		plan.setPlanConfiguration(pactPlan.getPlanConfiguration());
		
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
	public static OptimizedPlan createPreOptimizedPlan(Plan pactPlan) {
		GraphCreatingVisitor graphCreator = new GraphCreatingVisitor(null, -1, -1, -1, false);
		pactPlan.accept(graphCreator);
		OptimizedPlan optPlan = new OptimizedPlan(graphCreator.sources, graphCreator.sinks, graphCreator.con2node.values(),
				pactPlan.getJobName());
		optPlan.setPlanConfiguration(pactPlan.getPlanConfiguration());
		return optPlan;
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
	private static final class GraphCreatingVisitor implements Visitor<Contract> {
		private final Map<Contract, OptimizerNode> con2node; // map from the contract objects to their
																// corresponding optimizer nodes

		private final List<DataSourceNode> sources; // all data source nodes in the optimizer plan

		private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

		private final DataStatistics statistics; // used to access basic file statistics

		private final int maxMachines; // the maximum number of machines to use

		private final int defaultParallelism; // the default degree of parallelism

		private final int defaultIntraNodeParallelism; //

		private int id; // the incrementing id for the nodes.

		private final boolean computeEstimates; // flag indicating whether to compute additional info

		/**
		 * Creates a new node creating visitor.
		 */
		private GraphCreatingVisitor(DataStatistics statistics, int maxMachines, int defaultParallelism,
				int defaultIntraNodeParallelism, boolean computeEstimates) {
			this.con2node = new HashMap<Contract, OptimizerNode>();
			this.sources = new ArrayList<DataSourceNode>(4);
			this.sinks = new ArrayList<DataSinkNode>(2);

			this.statistics = statistics;

			this.maxMachines = maxMachines;
			this.defaultParallelism = defaultParallelism;
			this.defaultIntraNodeParallelism = defaultIntraNodeParallelism;

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
			if (c instanceof GenericDataSinkContract<?, ?>) {
				DataSinkNode dsn = new DataSinkNode((GenericDataSinkContract<?, ?>) c);
				sinks.add(dsn);
				n = dsn;
			} else if (c instanceof GenericDataSourceContract<?, ?>) {
				DataSourceNode dsn = new DataSourceNode((GenericDataSourceContract<?, ?>) c);
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
			par = par >= 1 ? par : this.defaultParallelism;

			// set the parallelism only if it has not been set before
			if (n.getDegreeOfParallelism() < 1) {
				n.setDegreeOfParallelism(par);
			}

			// check if we need to set the instance sharing accordingly such that
			// the maximum number of machines is not exceeded
			int tasksPerInstance = 1;
			if (maxMachines > 0) {
				int p = n.getDegreeOfParallelism();
				tasksPerInstance = (p / maxMachines) + (p % maxMachines == 0 ? 0 : 1);

				tasksPerInstance = Math.max(tasksPerInstance, this.defaultIntraNodeParallelism);
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

			// check if we have been here before
			if (n.getId() > 0) {
				return;
			}

			n.SetId(id++);

			// first connect to the predecessors
			n.setInputs(con2node);

			// now compute the output estimates
			if (computeEstimates) {
				n.computeOutputEstimates(this.statistics);
			}
		}

		public int getId() {
			return id;
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
	 * Utility class that traverses a plan to connect all nodes.
	 */
	private static final class NodeConnector implements Visitor<OptimizerNode> {
		private final Set<OptimizerNode> allNodes; // a set of all nodes in the optimizer plan

		/**
		 * Creates a new node connector.
		 */
		private NodeConnector() {
			this.allNodes = new HashSet<OptimizerNode>();
		}

		private void connectNodes(List<DataSinkNode> sinks) {
			
			// traverse the graph
			for (DataSinkNode node : sinks) {
				node.accept(this);
			}
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
	
	/**
	 * Utility class that traverses a plan to collect all nodes and add them to the OptimizedPlan.
	 * Besides collecting all nodes, this traversal assigns the memory to the nodes.
	 */
	private static final class PlanFinalizer implements Visitor<OptimizerNode> {
		private final Set<OptimizerNode> allNodes; // a set of all nodes in the optimizer plan

		private final List<DataSourceNode> sources; // all data source nodes in the optimizer plan

		private final List<DataSinkNode> sinks; // all data sink nodes in the optimizer plan

		private int memoryConsumers; // a counter of all memory consumers
		
		private int memoryPerInstance; // the amount of memory per instance

		/**
		 * Creates a new plan finalizer.
		 */
		private PlanFinalizer() {
			this.allNodes = new HashSet<OptimizerNode>();
			this.sources = new ArrayList<DataSourceNode>();
			this.sinks = new ArrayList<DataSinkNode>();
		}

		private OptimizedPlan createFinalPlan(List<DataSinkNode> sinks, String jobName, int memoryPerInstance) {
			
			LOG.debug("Available memory per instance: "+memoryPerInstance);
			
			this.memoryPerInstance = memoryPerInstance;
			this.memoryConsumers = 0;
			
			// traverse the graph
			for (DataSinkNode node : sinks) {
				node.accept(this);
			}

			// assign the memory to each node
			if (this.memoryConsumers > 0) {
				int memoryPerTask = this.memoryPerInstance / this.memoryConsumers;
				LOG.debug("Memory per consumer: "+memoryPerTask);
				for (OptimizerNode node : this.allNodes) {
					int consumerCount = node.getMemoryConsumerCount(); 
					if (consumerCount > 0) {
						node.setMemoryPerTask(memoryPerTask * consumerCount);
						LOG.debug("Assigned "+(memoryPerTask * consumerCount)+" MB to "+node.getPactContract().getName());
					}
					
					for (PactConnection conn : node.getOutgoingConnections()) {
						if(conn.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
							node.getPactContract().getParameters().setInteger(HistogramTask.HISTOGRAM_MEMORY, memoryPerTask);
							LOG.debug("Assigned "+(memoryPerTask)+" MB for histogram building during range partitioning");
						}
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
				// check for memory consuming temp connection
				switch(c.getTempMode()) {
					case NONE:
						// do nothing
						break;
					case TEMP_SENDER_SIDE:
						// reduce available memory
						this.memoryPerInstance -= PactCompiler.DEFAULT_TEMP_TASK_MEMORY * 
													c.getSourcePact().getDegreeOfParallelism();
						LOG.debug("Memory reduced to "+memoryPerInstance+ " due to TempTask");
						break;
					case TEMP_RECEIVER_SIDE:
						// reduce available memory
						this.memoryPerInstance -= PactCompiler.DEFAULT_TEMP_TASK_MEMORY * 
													c.getTargetPact().getDegreeOfParallelism();
						LOG.debug("Memory reduced to "+memoryPerInstance+ " due to TempTask");
						break;
				}

			}
			
			for (PactConnection conn : visitable.getOutgoingConnections()) {
				if(conn.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
					// One memory consumer for the histogram
					memoryConsumers += visitable.getInstancesPerMachine();
					//Reduce available memory because of temp task to avoid spilling
					this.memoryPerInstance -= PactCompiler.DEFAULT_TEMP_TASK_MEMORY *
					conn.getSourcePact().getDegreeOfParallelism();
					//TODO: is this correct reducing memory per INSTANCE by multiplying required
					//memory * the TOTAL DoP?
					LOG.debug("Memory reduced to "+memoryPerInstance+ " due to TempTask");
				}
			}

			if (visitable instanceof DataSinkNode) {
				sinks.add((DataSinkNode) visitable);
			} else if (visitable instanceof DataSourceNode) {
				sources.add((DataSourceNode) visitable);
			}

			// count the memory consumption
			memoryConsumers += visitable.getMemoryConsumerCount() * visitable.getInstancesPerMachine();
			
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

	/**
	 * Utility class to resolve pipeline deadlocks in non-tree graphs.
	 * Deadlocks can be resolved by inserting artificial dams (temping connections) or duplicating parts of the program.
	 * 
	 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
	 */
	private static final class DeadlockResolver implements Visitor<OptimizerNode> {
		
		// TODO: deadlock resolving should be integrated into the optimization and not done as a postprocessing
		
		private final boolean spillingActivated;
		private final HashSet<OptimizerNode> visitedNodes;
		
		private final List<PactConnection> deadlockConnection;
		
		public DeadlockResolver(boolean spillingActivated) {
			this.spillingActivated = spillingActivated;
			this.visitedNodes = new HashSet<OptimizerNode>();
			
			this.deadlockConnection = new ArrayList<PactConnection>();
		}
		
		public void resolveDeadlocks(List<DataSinkNode> sinks) {
			
			// traverse the graph
			for (DataSinkNode node : sinks) {
				node.accept(this);
			}

			for(PactConnection conn : deadlockConnection) {
				// unmark connection
				conn.setTempMode(TempMode.NONE);
				// resolve deadlock for connection
				resolveDeadlock(conn);
			}
			
		}

		/**
		 * Inserts temping connections where it is necessary.
		 * 
		 * @param nodesToVisit List of nodes that are visited in this recursive call. To process all nodes of the plan, 
		 *                     the method must be called with a list of all DataSinkNodes.
		 * @param visitedNodes Set of nodes that have already been visited. To process all nodes of the plan, 
		 *                     the method must be called with an empty set. 
		 */
		@Override
		public boolean preVisit(OptimizerNode node) {
			
			if(visitedNodes.contains(node)) 
				return false; 
			
			// mark that we have been here
			visitedNodes.add(node);
			
			// check if temp task is required
			for (PactConnection inConn : node.getIncomingConnections()) {
				// check if inConn is a blocked connection
				if (isBlockedConnection(inConn)) {
					// check if inConn needs to be temped
					if (mayCauseDeadlock(inConn)) {
						// mark connection as temped
						inConn.setTempMode(TempMode.TEMP_RECEIVER_SIDE);
						// insert connection into temp connection list
						deadlockConnection.add(inConn);
					}
				}
			}
			
			return true;
		}

		@Override
		public void postVisit(OptimizerNode visitable) {
			// do nothing
		}

		/**
		 * Checks whether a connection is blocked (waiting for other connection to be fully read).
		 * Blocked connections are:
		 * - Connection to Match PACTs that probe a HashTable
		 * - Connection to Match PACTs that provide sorted input
		 * - Connection to CoGroup PACTs that provide sorted input
		 * - Connection to Cross PACTs that are the outer loop of the nested-loop strategy
		 * Connections that are explicitly temping are never blocked!
		 * If the Nephele system was started with activated network spilling, data that cannot be forwarded to 
		 * the task is written to disk and later forwarded. 
		 * Hence, network channels are never blocking if network spilling is activated. 
		 * 
		 * @param conn
		 *        Connection that is checked for being a blocked connection.
		 * @return True if the connection is blocked, False otherwise.
		 */
		private boolean isBlockedConnection(PactConnection conn) {
			if (conn.getTempMode() != TempMode.NONE) {
				return false;
			}
			
			if (this.spillingActivated &&
					(
					conn.getShipStrategy() == ShipStrategy.BROADCAST ||
					conn.getShipStrategy() == ShipStrategy.PARTITION_HASH || 
					conn.getShipStrategy() == ShipStrategy.PARTITION_RANGE || 
					conn.getShipStrategy() == ShipStrategy.SFR
					)
				) {
				// network spilling is activated and shipping strategy will use network channels
				return false;
			}

			switch (conn.getTargetPact().getPactType()) {
			case Match:
				// test if hash strategy is used
				int inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
				switch (conn.getTargetPact().getLocalStrategy()) {
				case HYBRIDHASH_FIRST:
					// first input is build side
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case HYBRIDHASH_SECOND:
					// second input is build side
					if (inConnIdx == 0)
						return true;
					else
						return false;
				case MMHASH_FIRST:
					// first input is build side
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case MMHASH_SECOND:
					// second input is build side
					if (inConnIdx == 0)
						return true;
					else
						return false;
				case SORT_FIRST_MERGE:
					// first input is sorted
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case SORT_SECOND_MERGE:
					// second input is sorted
					if (inConnIdx == 0)
						return true;
					else
						return false;
				default:
					return false;
				}
			case Cogroup:
				inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
				switch (conn.getTargetPact().getLocalStrategy()) {
				case SORT_FIRST_MERGE:
					// first input is sorted
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case SORT_SECOND_MERGE:
					// second input is sorted
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
					// first input is fully read before processing (inner side)
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case NESTEDLOOP_STREAMED_OUTER_SECOND:
					// first input is fully read before processing (inner side)
					if (inConnIdx == 1)
						return true;
					else
						return false;
				case NESTEDLOOP_BLOCKED_OUTER_FIRST:
					// second input is fully read before processing (inner side)
					if (inConnIdx == 0)
						return true;
					else
						return false;
				case NESTEDLOOP_STREAMED_OUTER_FIRST:
					// second input is fully read before processing (inner side)
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
				switch (conn.getTargetPact().getLocalStrategy()) {
				case SORT: 
					// sort reads everything before processing
					return true;
				}
				return true;
			case Match:
				int inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
				switch (conn.getTargetPact().getLocalStrategy()) {
				case SORT_BOTH_MERGE:
					// sort reads everything before processing
					return true;
				case SORT_FIRST_MERGE:
					if (inConnIdx == 0)
						// input is sorted
						return true;
					else
						// input is NOT sorted
						return false;
				case SORT_SECOND_MERGE:
					if (inConnIdx == 1)
						// input is sorted
						return true;
					else
						// input is NOT sorted
						return false;
				case HYBRIDHASH_FIRST:
					if (inConnIdx == 0)
						// input is put into hashtable
						return true;
					else
						// input is NOT put into hashtable
						return false;
				case HYBRIDHASH_SECOND:
					if (inConnIdx == 1)
						// input is put into hashtable
						return true;
					else
						// input is NOT put into hashtable
						return false;
				case MMHASH_FIRST:
					if (inConnIdx == 0)
						// input is put into hashtable
						return true;
					else
						// input is NOT put into hashtable
						return false;
				case MMHASH_SECOND:
					if (inConnIdx == 1)
						// input is put into hashtable
						return true;
					else
						// input is NOT put into hashtable
						return false;
				default:
					return false;
				}
			case Cross:
				inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
				switch (conn.getTargetPact().getLocalStrategy()) {
				case NESTEDLOOP_BLOCKED_OUTER_SECOND:
					if (inConnIdx == 0)
						// input is put read into resettable iterator
						return true;
					else
						// input is put block-wise streamed over resettable iterator
						return false;
				case NESTEDLOOP_STREAMED_OUTER_SECOND:
					if (inConnIdx == 0)
						// input is put read into resettable iterator
						return true;
					else
						// input is put block-wise streamed over resettable iterator
						return false;
				case NESTEDLOOP_BLOCKED_OUTER_FIRST:
					if (inConnIdx == 1)
						// input is put read into resettable iterator
						return true;
					else
						// input is put block-wise streamed over resettable iterator
						return false;
				case NESTEDLOOP_STREAMED_OUTER_FIRST:
					if (inConnIdx == 1)
						// input is put read into resettable iterator
						return true;
					else
						// input is put block-wise streamed over resettable iterator
						return false;
				}
			case Cogroup:
				inConnIdx = conn.getTargetPact().getIncomingConnections().indexOf(conn);
				switch (conn.getTargetPact().getLocalStrategy()) {
				case SORT_BOTH_MERGE:
					// sort reads everything before processing
					return true;
				case SORT_FIRST_MERGE:
					if (inConnIdx == 0)
						// input is sorted
						return true;
					else
						// input is NOT sorted
						return false;
				case SORT_SECOND_MERGE:
					if (inConnIdx == 1)
						// input is sorted
						return true;
					else
						// input is NOT sorted
						return false;
				}
			default:
				return false;
			}
		}

		/**
		 * Checks whether a blocked connection may cause a deadlock.
		 * A blocked connection is a dam since it has to wait until the input of the other connection is fully read
		 * (probe-side for hash strategies, streaming-side for nested-loop strategy, etc.).
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
		 *        Blocked connection that should be checked for deadlock potential
		 * @return True if the blocked connection may cause a deadlock, False otherwise.
		 */
		private boolean mayCauseDeadlock(PactConnection conn) {
			if (isTempingConnection(conn)) {
				return false;
			} else {
				if (conn.getSourcePact().getOutgoingConnections().size() > 1) {
					return true;
				} else {
					for (PactConnection inConn : conn.getSourcePact().getIncomingConnections()) {
						if (mayCauseDeadlock(inConn) == true) {
							return true;
						}
					}
					return false;
				}
			}
		}

		private void resolveDeadlock(PactConnection conn) {
			// TODO: decide smarter where to put temp (sender/receiver, connection)
			
			OptimizerNode sourcePact = conn.getSourcePact();
			OptimizerNode targetPact = conn.getTargetPact();
			
			if(sourcePact instanceof DataSourceNode) {
				// instead of temping connection duplicate DataSourceNode
				
				// duplicate DataSourceNode
				DataSourceNode duplicateDataSource;
				if(sourcePact.getPactContract() instanceof FileDataSinkContract<?, ?>) {
					duplicateDataSource = new DataSourceNode((FileDataSourceContract<?, ?>)sourcePact.getPactContract());
				} else {
					duplicateDataSource = new DataSourceNode((GenericDataSourceContract<?, ?>)sourcePact.getPactContract());
				}
				duplicateDataSource.setDegreeOfParallelism(sourcePact.getDegreeOfParallelism());
				duplicateDataSource.setInstancesPerMachine(sourcePact.getInstancesPerMachine());
				// create new connection
				PactConnection newConn = new PactConnection(conn, duplicateDataSource, targetPact);
				
				// remove connection from original DataSourceNode
				sourcePact.getOutgoingConnections().remove(conn);
				// add new connection to new DataSourceNode
				duplicateDataSource.addOutgoingConnection(newConn);
				// replace old connection with new connection
				if(targetPact instanceof SingleInputNode) {
					((SingleInputNode)targetPact).setInputConnection(newConn);
				} else if(targetPact instanceof TwoInputNode) {
					if(((TwoInputNode)targetPact).getFirstInputConnection() == conn) {
						((TwoInputNode)targetPact).setFirstInputConnection(newConn);
					} else {
						((TwoInputNode)targetPact).setSecondInputConnection(newConn);
					}
				}
				
			} else if(sourcePact.getIncomingConnections().size() > 1) {
				// node has more than one incoming connection
				// can't push temp further down, insert temping here
				conn.setTempMode(TempMode.TEMP_RECEIVER_SIDE);
			} else if(sourcePact.getOutgoingConnections().size() > 1) {
				// node has more than one outgoing connection
				// this is the reason for the temp, insert temping here
				conn.setTempMode(TempMode.TEMP_RECEIVER_SIDE);
			} else {
				PactConnection predConn = sourcePact.getIncomingConnections().get(0); 
				long curSize = sourcePact.getEstimatedOutputSize();
				long predSize = predConn.getSourcePact().getEstimatedOutputSize();
				
				if(curSize < predSize) {
					// this conn will ship less data than the preceding conn
					// insert temping here
					conn.setTempMode(TempMode.TEMP_RECEIVER_SIDE);
				} else {
					// this conn ships same or more data than preceding conn
					// insert temp further ahead
					resolveDeadlock(predConn);
				}
			}
		}
		
	}

	// ------------------------------------------------------------------------
	// Miscellaneous
	// ------------------------------------------------------------------------

	/**
	 * This utility method picks the instance type to be used for scheduling PACT processor
	 * instances.
	 * <p>
	 * 
	 * @param types The available types.
	 * @return The type to be used for scheduling.
	 * 
	 * @throws CompilerException
	 * @throws IllegalArgumentException
	 */
	private InstanceTypeDescription getType(Map<InstanceType, InstanceTypeDescription> types)
	throws CompilerException
	{
		if (types == null || types.size() < 1) {
			throw new IllegalArgumentException("No instance type found.");
		}
		
		InstanceTypeDescription retValue = null;
		long totalMemory = 0;
		int numInstances = 0;
		
		final Iterator<InstanceTypeDescription> it = types.values().iterator();
		while(it.hasNext())
		{
			final InstanceTypeDescription descr = it.next();
			
			// skip instances for which no hardware description is available
			// this means typically that no 
			if (descr.getHardwareDescription() == null || descr.getInstanceType() == null) {
				continue;
			}
			
			final int curInstances = descr.getMaximumNumberOfAvailableInstances();
			final long curMemory = curInstances * descr.getHardwareDescription().getSizeOfFreeMemory();
			
			// get, if first, or if it has more instances and not less memory, or if it has significantly more memory
			// and the same number of cores still
			if ( (retValue == null) ||
				 (curInstances > numInstances && (int) (curMemory * 1.2f) > totalMemory) ||
				 (curInstances * retValue.getInstanceType().getNumberOfCores() >= numInstances && 
							(int) (curMemory * 1.5f) > totalMemory)
				)
			{
				retValue = descr;
				numInstances = curInstances;
				totalMemory = curMemory;
			}
		}
		
		if (retValue == null) {
			throw new CompilerException("No instance currently registered at the job-manager. Retry later.\n" +
				"If the system has recently started, it may take a few seconds until the instances register.");
		}
		
		return retValue;
	}
	
	/**
	 * Utility class for an asynchronous connection to the job manager to determine the available instances.
	 */
	private static final class JobManagerConnector implements Runnable
	{
		private static final long MAX_MILLIS_TO_WAIT = 10000;
		
		private final InetSocketAddress jobManagerAddress;
		
		private final Object lock = new Object();
		
		private volatile Map<InstanceType, InstanceTypeDescription> instances;
		
		private volatile Throwable error;
		
		
		private JobManagerConnector(InetSocketAddress jobManagerAddress)
		{
			this.jobManagerAddress = jobManagerAddress;
		}
		
		
		public void waitForCompletion() throws Throwable
		{
			long start = System.currentTimeMillis();
			long remaining = MAX_MILLIS_TO_WAIT;
			
			if (this.error != null) {
				throw this.error;
			}
			if (this.instances != null) {
				return;
			}
			
			do {
				try {
					synchronized (this.lock) {
						this.lock.wait(remaining);
					}
				} catch (InterruptedException iex) {}
			}
			while (this.error == null && this.instances == null &&
					(remaining = MAX_MILLIS_TO_WAIT + start - System.currentTimeMillis()) > 0);
			
			if (this.error != null) {
				throw this.error;
			}
			if (this.instances != null) {
				return;
			}
			
			// try to forcefully shut this thread down
			throw new IOException("Connection timed out.");
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run()
		{
			ExtendedManagementProtocol jobManagerConnection = null;

			try {
				jobManagerConnection = (ExtendedManagementProtocol) RPC.getProxy(ExtendedManagementProtocol.class,
					this.jobManagerAddress, NetUtils.getSocketFactory());

				this.instances = jobManagerConnection.getMapOfAvailableInstanceTypes();
				if (this.instances == null) {
					throw new IOException("Returned instance map was <null>");
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
			finally {
				// first of all, signal completion
				synchronized (this.lock) {
					this.lock.notifyAll();
				}
				
				if (jobManagerConnection != null) {
					try {
						RPC.stopProxy(jobManagerConnection);
					} catch (Throwable t) {
						LOG.error("Could not cleanly shut down connection from compiler to job manager,", t);
					}
				}
				jobManagerConnection = null;
			}
		}
		
	}
}
