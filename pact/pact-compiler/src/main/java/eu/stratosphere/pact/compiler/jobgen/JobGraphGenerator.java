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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.nephele.template.AbstractInputTask;
import eu.stratosphere.pact.common.contract.AbstractPact;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.CoGroupNode;
import eu.stratosphere.pact.compiler.plan.CombinerNode;
import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.compiler.plan.DataSourceNode;
import eu.stratosphere.pact.compiler.plan.MapNode;
import eu.stratosphere.pact.compiler.plan.MatchNode;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.ReduceNode;
import eu.stratosphere.pact.compiler.plan.UnionNode;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.CoGroupTask;
import eu.stratosphere.pact.runtime.task.CombineTask;
import eu.stratosphere.pact.runtime.task.CrossTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.MapTask;
import eu.stratosphere.pact.runtime.task.MatchTask;
import eu.stratosphere.pact.runtime.task.ReduceTask;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.chaining.ChainedCombineTask;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapTask;
import eu.stratosphere.pact.runtime.task.chaining.ChainedTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * This component translates an optimized PACT plan into a nephele schedule. The
 * translation is a one to one mapping, setting and configuring all nephele
 * parameters. The parameters are set with with values that are either indicated
 * in the PACT program directly, are set by the PACT compiler, or with default values.
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class JobGraphGenerator implements Visitor<OptimizerNode> {
	
	private static final Log LOG = LogFactory.getLog(JobGraphGenerator.class);
	
	private static final int DEFAULT_MERGE_FACTOR = 64; // the number of streams to merge at once
	
	// ------------------------------------------------------------------------

	private JobGraph jobGraph; // the job that is currently built

	private Map<OptimizerNode, AbstractJobVertex> vertices; // a map from optimizer nodes to nephele vertices
	
	private Map<OptimizerNode, TaskInChain> chainedTasks; // a map from optimizer nodes to nephele vertices
	
	private List<TaskInChain> chainedTasksInSequence;
	
	private List<AbstractJobVertex> auxVertices; // auxiliary vertices which are added during job graph generation

	private AbstractJobVertex maxDegreeVertex; // the vertex with the highest degree of parallelism
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new job graph generator that uses the default values for its resource configuration.
	 */
	public JobGraphGenerator() {
	}

	/**
	 * Translates a {@link eu.stratosphere.pact.compiler.plan.OptimizedPlan} into a
	 * {@link eu.stratosphere.nephele.jobgraph.JobGraph}.
	 * This is an 1-to-1 mapping. No optimization whatsoever is applied.
	 * 
	 * @param pactPlan
	 *        Optimized PACT plan that is translated into a JobGraph.
	 * @return JobGraph generated from PACT plan.
	 */
	public JobGraph compileJobGraph(OptimizedPlan pactPlan)
	{
		this.jobGraph = new JobGraph(pactPlan.getJobName());
		this.vertices = new HashMap<OptimizerNode, AbstractJobVertex>();
		this.chainedTasks = new HashMap<OptimizerNode, TaskInChain>();
		this.chainedTasksInSequence = new ArrayList<TaskInChain>();
		this.auxVertices = new ArrayList<AbstractJobVertex>();
		this.maxDegreeVertex = null;
		
		// set Nephele JobGraph config
		pactPlan.getPlanConfiguration().extractNepheleConfiguration(this.jobGraph.getJobConfiguration());
		
		// generate Nephele job graph
		pactPlan.accept(this);
		
		// now that the traversal is done, we have the chained tasks write their configs into their
		// parents' configurations
		for (int i = 0; i < this.chainedTasksInSequence.size(); i++) {
			TaskInChain tic = this.chainedTasksInSequence.get(i);
			TaskConfig t = new TaskConfig(tic.getContainingVertex().getConfiguration());
			t.addChainedTask(tic.getChainedTask(), tic.getTaskConfig(), tic.getTaskName());
		}

		// now that all have been created, make sure that all share their instances with the one
		// with the highest degree of parallelism
		if (pactPlan.getInstanceTypeName() != null) {
			this.maxDegreeVertex.setInstanceType(pactPlan.getInstanceTypeName());
		} else {
			LOG.warn("No instance type assigned to Nephele JobVertex.");
		}
		for (AbstractJobVertex vertex : this.vertices.values()) {
			if (vertex == this.maxDegreeVertex) {
				continue;
			}
			vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
		}
		
		for (AbstractJobVertex vertex : this.auxVertices) {
			if (vertex == this.maxDegreeVertex) {
				continue;
			}
			vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
		}
		

		JobGraph graph = this.jobGraph;

		// release all references again
		this.maxDegreeVertex = null;
		this.vertices = null;
		this.chainedTasks = null;
		this.chainedTasksInSequence = null;
		this.auxVertices = null;
		this.jobGraph = null;

		// return job graph
		return graph;
	}
	
	/**
	 * This methods implements the pre-visiting during a depth-first traversal. It create the job vertex and
	 * sets local strategy.
	 * 
	 * @param node
	 *        The node that is currently processed.
	 * @return True, if the visitor should descend to the node's children, false if not.
	 * @see eu.stratosphere.pact.common.plan.Visitor#preVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public boolean preVisit(OptimizerNode node)
	{
		// check if we have visited this node before. in non-tree graphs, this happens
		if (this.vertices.containsKey(node) || this.chainedTasks.containsKey(node)) {
			// return false to prevent further descend
			return false;
		}

		// the vertex to be created for the current node
		final AbstractJobVertex vertex;

		try {
			switch (node.getPactType()) {
			case Map:
				vertex = generateMapVertex(node);
				break;
			case Reduce:
				if (node instanceof ReduceNode) {
					vertex = generateReduceVertex((ReduceNode) node);
				} else if (node instanceof CombinerNode) {
					vertex = generateCombineVertex((CombinerNode) node);
				} else {
					throw new CompilerException("Wrong node type for PACT type 'Reduce': " + node.getClass().getName());
				}
				break;
			case Match:
				vertex = generateMatchVertex(node);
				break;
			case Cross:
				vertex = generateCrossVertex(node);
				break;
			case Cogroup:
				vertex = generateCoGroupVertex(node);
				break;
			case DataSource:
				vertex = generateDataSourceVertex(node);
				break;
			case DataSink:
				vertex = generateDataSinkVertex(node);
				break;
			case Union:
				vertex = null;
				break;
			default:
				throw new Exception("Unknown PACT type: " + node.getPactType());
			}
		}
		catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}
		
		// check if a vertex was created, or if it was chained
		if (vertex != null) {
			// set degree of parallelism
			int pd = node.getDegreeOfParallelism();
			vertex.setNumberOfSubtasks(pd);
	
			// check whether this is the vertex with the highest degree of parallelism
			if (this.maxDegreeVertex == null || this.maxDegreeVertex.getNumberOfSubtasks() < pd) {
				this.maxDegreeVertex = vertex;
			}
	
			// set the number of tasks per instance
			if (node.getInstancesPerMachine() >= 1) {
				vertex.setNumberOfSubtasksPerInstance(node.getInstancesPerMachine());
			}
	
			// store in the map
			this.vertices.put(node, vertex);
		}

		// returning true causes deeper descend
		return true;
	}

	/**
	 * This method implements the post-visit during the depth-first traversal. When the post visit happens,
	 * all of the descendants have been processed, so this method connects all of the current node's
	 * predecessors to the current node.
	 * 
	 * @param node
	 *        The node currently processed during the post-visit.
	 * @see eu.stratosphere.pact.common.plan.Visitor#postVisit(eu.stratosphere.pact.common.plan.Visitable)
	 */
	@Override
	public void postVisit(OptimizerNode node)
	{
		try {
			
			//do nothing for union nodes
			if (node instanceof UnionNode) {
				return;
			}
			
			// get pact vertex
			AbstractJobVertex inputVertex = this.vertices.get(node);
			List<PactConnection> inConns = node.getIncomingConnections();

			if (inConns == null) {
				// data source
				return;
			}

			// check whether this node has its own task, or is chained to another one
			if (inputVertex == null) {

				// node's task is chained in another task
				if (inConns.size() != 1) {
					throw new IllegalStateException("Chained task with more than one input!");
				}
				PactConnection inConn = inConns.get(0);

				final TaskInChain chainedTask = this.chainedTasks.get(node);
				AbstractJobVertex container = chainedTask.getContainingVertex();
				
				if (container == null) {
					final PactConnection connection = inConn;
					final OptimizerNode sourceNode = connection.getSourcePact();
					container = this.vertices.get(sourceNode);
					if (container == null) {
						// predecessor is itself chained
						container = this.chainedTasks.get(sourceNode).getContainingVertex();
						if (container == null)
							throw new IllegalStateException("Chained task predecessor has not been assigned its containing vertex.");
					} else {
						// predecessor is a proper task job vertex and this is the first chained task. add a forward connection entry.
						new TaskConfig(container.getConfiguration()).addOutputShipStrategy(ShipStrategy.FORWARD);
					}
					chainedTask.setContainingVertex(container);
				}
				
				this.chainedTasksInSequence.add(chainedTask);
				return;
			}
			
			// this task it not chained.
			final TaskConfig inputVertexConfig = new TaskConfig(inputVertex.getConfiguration());

			int inputIndex = 1;
			for(PactConnection input : inConns) {
				
				for (PactConnection inConn : getConnectionsOfInput(input)) {
					final OptimizerNode sourceNode = inConn.getSourcePact();
					AbstractJobVertex outputVertex = this.vertices.get(sourceNode);
					TaskConfig outputVertexConfig;
	
					if (outputVertex == null) {
						// this predecessor is chained to another task
						final TaskInChain chainedTask = this.chainedTasks.get(sourceNode);
						if (chainedTask.getContainingVertex() == null)
							throw new IllegalStateException("Chained task predecessor has not been assigned its containing vertex.");
						outputVertex = chainedTask.getContainingVertex();
						outputVertexConfig = chainedTask.getTaskConfig();
					} else {
						outputVertexConfig = new TaskConfig(outputVertex.getConfiguration());
					}
	
					switch (inConn.getShipStrategy()) {
					case FORWARD:
						connectWithForwardStrategy(inConn, inputIndex, outputVertex, outputVertexConfig, inputVertex, inputVertexConfig);
						break;
					case PARTITION_LOCAL_HASH:
					case PARTITION_HASH:
					case PARTITION_RANGE:
						connectWithPartitionStrategy(inConn, inputIndex, outputVertex, outputVertexConfig, inputVertex, inputVertexConfig);
						break;
					case BROADCAST:
						connectWithBroadcastStrategy(inConn, inputIndex, outputVertex, outputVertexConfig, inputVertex, inputVertexConfig);
						break;
					case SFR:
						connectWithSFRStrategy(inConn, inputIndex, outputVertex, outputVertexConfig, inputVertex, inputVertexConfig);
					default:
						throw new Exception("Invalid ship strategy: " + inConn.getShipStrategy());
					}
				}
				
				++inputIndex;
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}
	}
	
	private List<PactConnection> getConnectionsOfInput(PactConnection connection) {
		final OptimizerNode input = connection.getSourcePact();
		List<PactConnection> inConnOfInput;
		if (input instanceof UnionNode) {
			//TODO see if there is a nicer solution
			inConnOfInput = new LinkedList<PactConnection>();
			//create new connections between union inputs and target with properties of 
			//connection between target and union
			for (PactConnection inConn : input.getIncomingConnections()) {
				inConnOfInput.add(new PactConnection(connection, inConn.getSourcePact(), connection.getTargetPact()));
			}
		}
		else {
			inConnOfInput = Collections.singletonList(connection);
		}
		return inConnOfInput;
	}
	
	// ------------------------------------------------------------------------
	// Methods for creating individual vertices
	// ------------------------------------------------------------------------
	
	/**
	 * @param mapNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateMapVertex(OptimizerNode mapNode) throws CompilerException
	{
		final MapContract mc = ((MapNode) mapNode).getPactContract();
		final JobTaskVertex mapVertex;
		final TaskConfig mapConfig;
		
		if (isChainable(mapNode)) {
			mapVertex = null;
			mapConfig = new TaskConfig(new Configuration());
			
			this.chainedTasks.put(mapNode, new TaskInChain(ChainedMapTask.class, mapConfig, mc.getName()));
		}
		else {
			// create task vertex
			mapVertex = new JobTaskVertex(mapNode.getPactContract().getName(), this.jobGraph);
			// set task class
			mapVertex.setTaskClass(MapTask.class);
			// get task configuration object
			mapConfig = new TaskConfig(mapVertex.getConfiguration());
		}
		
		// set user code class
		mapConfig.setStubClass(mc.getUserCodeClass());
	
		// set local strategy
		switch (mapNode.getLocalStrategy()) {
		case NONE:
			mapConfig.setLocalStrategy(LocalStrategy.NONE);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'Map' (" + mapNode.getName() + "): "
				+ mapNode.getLocalStrategy());
		}
	
		// forward stub parameters to task and stub
		mapConfig.setStubParameters(mapNode.getPactContract().getParameters());

		return mapVertex;
	}

	/**
	 * @param combineNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateCombineVertex(CombinerNode combineNode) throws CompilerException
	{
		final ReduceContract rc = combineNode.getPactContract();
		final JobTaskVertex combineVertex;
		final TaskConfig combineConfig;
		
		// check if the combiner is chained
		if (isChainable(combineNode)) {
			combineVertex = null;
			combineConfig = new TaskConfig(new Configuration());
			this.chainedTasks.put(combineNode, new TaskInChain(ChainedCombineTask.class,
											combineConfig, "Combiner for " + rc.getName()));
		} else {
			combineVertex = new JobTaskVertex("Combiner for " + combineNode.getPactContract().getName(), this.jobGraph);
			combineVertex.setTaskClass(CombineTask.class);
			combineConfig = new TaskConfig(combineVertex.getConfiguration());
		}
		
		combineConfig.setStubClass(combineNode.getPactContract().getUserCodeClass());

		// we have currently only one strategy for combiners
		combineConfig.setLocalStrategy(LocalStrategy.COMBININGSORT);
		
		final Ordering secondaryOrder = combineNode.getPactContract().getSecondaryOrder();
		if (secondaryOrder == null) {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(combineConfig.getConfiguration(),
				combineConfig.getPrefixForInputParameters(0),
				combineNode.getPactContract().getKeyColumnNumbers(0), combineNode.getPactContract().getKeyClasses());
		} else {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(combineConfig.getConfiguration(),
				combineConfig.getPrefixForInputParameters(0),
				combineNode.getPactContract().getKeyColumnNumbers(0), combineNode.getPactContract().getKeyClasses(),
				secondaryOrder.getFieldPositions(), secondaryOrder.getTypes());
		}

		// assign the memory
		assignMemory(combineConfig, combineNode.getMemoryPerTask());

		// forward stub parameters to task and stub
		combineConfig.setStubParameters(combineNode.getPactContract().getParameters());

		return combineVertex;
	}

	/**
	 * @param reduceNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateReduceVertex(ReduceNode reduceNode) throws CompilerException
	{
		// create task vertex
		JobTaskVertex reduceVertex = new JobTaskVertex(reduceNode.getPactContract().getName(), this.jobGraph);
		// set task class
		reduceVertex.setTaskClass(ReduceTask.class);

		// get task configuration object
		TaskConfig reduceConfig = new TaskConfig(reduceVertex.getConfiguration());
		// set user code class
		reduceConfig.setStubClass(reduceNode.getPactContract().getUserCodeClass());
		
		// set contract's key information
		final Ordering secondaryOrder = reduceNode.getPactContract().getSecondaryOrder();
		if (secondaryOrder == null) {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(reduceConfig.getConfiguration(),
				reduceConfig.getPrefixForInputParameters(0),
				reduceNode.getPactContract().getKeyColumnNumbers(0), reduceNode.getPactContract().getKeyClasses());
		} else {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(reduceConfig.getConfiguration(),
				reduceConfig.getPrefixForInputParameters(0),
				reduceNode.getPactContract().getKeyColumnNumbers(0), reduceNode.getPactContract().getKeyClasses(),
				secondaryOrder.getFieldPositions(), secondaryOrder.getTypes());
		}

		// set local strategy
		switch (reduceNode.getLocalStrategy()) {
		case SORT:
			reduceConfig.setLocalStrategy(LocalStrategy.SORT);
			break;
		case COMBININGSORT:
			reduceConfig.setLocalStrategy(LocalStrategy.COMBININGSORT);
			break;
		case NONE:
			reduceConfig.setLocalStrategy(LocalStrategy.NONE);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'Reduce' (" + reduceNode.getName() + "): "
				+ reduceNode.getLocalStrategy());
		}

		// assign the memory
		assignMemory(reduceConfig, reduceNode.getMemoryPerTask());

		// forward stub parameters to task and stub
		reduceConfig.setStubParameters(reduceNode.getPactContract().getParameters());

		return reduceVertex;
	}

	/**
	 * @param matchNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateMatchVertex(OptimizerNode matchNode) throws CompilerException
	{
		MatchContract matchContract = ((MatchNode) matchNode).getPactContract();
		
		// create task vertex
		JobTaskVertex matchVertex = new JobTaskVertex(matchNode.getPactContract().getName(), this.jobGraph);
		
		// get task configuration object
		TaskConfig matchConfig = new TaskConfig(matchVertex.getConfiguration());
		
		matchConfig.setStubClass(matchNode.getPactContract().getUserCodeClass());
		
		// write key parameters
		PactRecordComparatorFactory.writeComparatorSetupToConfig(matchConfig.getConfiguration(),
			matchConfig.getPrefixForInputParameters(0),
			matchContract.getKeyColumnNumbers(0), matchContract.getKeyClasses());
		
		PactRecordComparatorFactory.writeComparatorSetupToConfig(matchConfig.getConfiguration(),
			matchConfig.getPrefixForInputParameters(1),
			matchContract.getKeyColumnNumbers(1), matchContract.getKeyClasses());

		switch (matchNode.getLocalStrategy())
		{
		case SORT_BOTH_MERGE:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			break;
		case SORT_FIRST_MERGE:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			break;
		case SORT_SECOND_MERGE:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			break;
		case MERGE:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.MERGE);
			break;
		case HYBRIDHASH_FIRST:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
			break;
		case HYBRIDHASH_SECOND:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
			break;
		case MMHASH_FIRST:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.MMHASH_FIRST);
			break;
		case MMHASH_SECOND:
			matchVertex.setTaskClass(MatchTask.class);
			matchConfig.setLocalStrategy(LocalStrategy.MMHASH_SECOND);
			break;
//		case SORT_SELF_NESTEDLOOP:
//			matchVertex.setTaskClass(SelfMatchTask.class);
//			matchConfig.setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
//			break;
//		case SELF_NESTEDLOOP:
//			matchVertex.setTaskClass(SelfMatchTask.class);
//			matchConfig.setLocalStrategy(LocalStrategy.SELF_NESTEDLOOP);
//			break;
		default:
			throw new CompilerException("Invalid local strategy for 'Match' (" + matchNode.getName() + "): "
				+ matchNode.getLocalStrategy());
		}

		// assign the memory
		assignMemory(matchConfig, matchNode.getMemoryPerTask());

		// forward stub parameters to task and stub
		matchConfig.setStubParameters(matchNode.getPactContract().getParameters());

		return matchVertex;
	}

	/**
	 * @param crossNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateCrossVertex(OptimizerNode crossNode) throws CompilerException {
		// create task vertex
		JobTaskVertex crossVertex = new JobTaskVertex(crossNode.getPactContract().getName(), this.jobGraph);
		// set task class
		crossVertex.setTaskClass(CrossTask.class);

		// get task configuration object
		TaskConfig crossConfig = new TaskConfig(crossVertex.getConfiguration());
		// set user code class
		crossConfig.setStubClass(crossNode.getPactContract().getUserCodeClass());

		// set local strategy
		switch (crossNode.getLocalStrategy()) {
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			crossConfig.setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			crossConfig.setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			crossConfig.setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			crossConfig.setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'Cross' (" + crossNode.getName() + "): "
				+ crossNode.getLocalStrategy());
		}

		assignMemory(crossConfig, crossNode.getMemoryPerTask());

		// forward stub parameters to task and stub
		crossConfig.setStubParameters(crossNode.getPactContract().getParameters());

		return crossVertex;
	}

	/**
	 * @param coGroupNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateCoGroupVertex(OptimizerNode coGroupNode) throws CompilerException {

		CoGroupContract coGroupContract = ((CoGroupNode) coGroupNode).getPactContract();
		
		// create task vertex
		JobTaskVertex coGroupVertex = new JobTaskVertex(coGroupNode.getPactContract().getName(), this.jobGraph);
		// set task class
		coGroupVertex.setTaskClass(CoGroupTask.class);

		// get task configuration object
		TaskConfig coGroupConfig = new TaskConfig(coGroupVertex.getConfiguration());
		// set user code class
		coGroupConfig.setStubClass(coGroupNode.getPactContract().getUserCodeClass());
		
		// set contract's key information
		final Ordering secondaryOrder1 = coGroupContract.getSecondaryOrder(0);
		if (secondaryOrder1 == null) {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(coGroupConfig.getConfiguration(),
				coGroupConfig.getPrefixForInputParameters(0),
				coGroupContract.getKeyColumnNumbers(0), coGroupContract.getKeyClasses());
		} else {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(coGroupConfig.getConfiguration(),
				coGroupConfig.getPrefixForInputParameters(0),
				coGroupContract.getKeyColumnNumbers(0), coGroupContract.getKeyClasses(),
				secondaryOrder1.getFieldPositions(), secondaryOrder1.getTypes());
		}
		
		// set contract's key information
		final Ordering secondaryOrder2 = coGroupContract.getSecondaryOrder(1);
		if (secondaryOrder2 == null) {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(coGroupConfig.getConfiguration(),
				coGroupConfig.getPrefixForInputParameters(1),
				coGroupContract.getKeyColumnNumbers(1), coGroupContract.getKeyClasses());
		} else {
			PactRecordComparatorFactory.writeComparatorSetupToConfig(coGroupConfig.getConfiguration(),
				coGroupConfig.getPrefixForInputParameters(1),
				coGroupContract.getKeyColumnNumbers(1), coGroupContract.getKeyClasses(),
				secondaryOrder2.getFieldPositions(), secondaryOrder2.getTypes());
		}

		// set local strategy
		switch (coGroupNode.getLocalStrategy()) {
		case SORT_BOTH_MERGE:
			coGroupConfig.setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			break;
		case SORT_FIRST_MERGE:
			coGroupConfig.setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			break;
		case SORT_SECOND_MERGE:
			coGroupConfig.setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			break;
		case MERGE:
			coGroupConfig.setLocalStrategy(LocalStrategy.MERGE);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'CoGroup' (" + coGroupNode.getName() + "): "
				+ coGroupNode.getLocalStrategy());
		}

		// assign the memory
		assignMemory(coGroupConfig, coGroupNode.getMemoryPerTask());

		// forward stub parameters to task and stub
		coGroupConfig.setStubParameters(coGroupNode.getPactContract().getParameters());

		return coGroupVertex;
	}

	/**
	 * @param sourceNode
	 * @return
	 * @throws CompilerException
	 */
	private JobInputVertex generateDataSourceVertex(OptimizerNode sourceNode) throws CompilerException
	{
		DataSourceNode dsn = (DataSourceNode) sourceNode;
		GenericDataSource<?> contract = dsn.getPactContract();

		// create task vertex
		JobInputVertex sourceVertex = new JobInputVertex(contract.getName(), this.jobGraph);
		// set task class
		@SuppressWarnings("unchecked")
		final Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
		sourceVertex.setInputClass(clazz);

		// get task configuration object
		TaskConfig sourceConfig = new TaskConfig(sourceVertex.getConfiguration());
		// set user code class
		sourceConfig.setStubClass(contract.getUserCodeClass());
		// forward stub parameters to task and data format
		sourceConfig.setStubParameters(contract.getParameters());

		// set local strategy
		switch (sourceNode.getLocalStrategy()) {
		case NONE:
			sourceConfig.setLocalStrategy(LocalStrategy.NONE);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'DataSource'(" + sourceNode.getName() + "): "
				+ sourceNode.getLocalStrategy());
		}

		return sourceVertex;
	}

	/**
	 * @param sinkNode
	 * @return
	 * @throws CompilerException
	 */
	private AbstractJobOutputVertex generateDataSinkVertex(OptimizerNode sinkNode) throws CompilerException
	{
		DataSinkNode sNode = (DataSinkNode) sinkNode;
		GenericDataSink sinkContract = sNode.getPactContract();
		
		// create task vertex
		JobOutputVertex sinkVertex = new JobOutputVertex(sinkNode.getPactContract().getName(), this.jobGraph);
		// set task class
		sinkVertex.setOutputClass(DataSinkTask.class);
		
		// set the degree-of-parallelism into the config to have it available during the output path checking.
		sinkVertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, sinkNode.getDegreeOfParallelism());
		
		// get task configuration object
		TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
		// set user code class
		sinkConfig.setStubClass(sinkContract.getUserCodeClass());
		// forward stub parameters to task and data format
		sinkConfig.setStubParameters(sinkContract.getParameters());
		
		if (sNode.getLocalStrategy() == LocalStrategy.SORT) {
			assignMemory(sinkConfig, sinkNode.getMemoryPerTask());
			
			PactRecordComparatorFactory.writeComparatorSetupToConfig(sinkConfig.getConfiguration(),
				sinkConfig.getPrefixForInputParameters(0),
				sNode.getPactContract().getLocalOrder().getFieldPositions(), sNode.getPactContract().getLocalOrder().getTypes());
		}

		// set local strategy
		switch (sinkNode.getLocalStrategy()) {
		case NONE:
			sinkConfig.setLocalStrategy(LocalStrategy.NONE);
			break;
		case SORT:
			sinkConfig.setLocalStrategy(LocalStrategy.SORT);
			break;
		default:
			throw new CompilerException("Invalid local strategy for 'DataSink' (" + sinkNode.getName() + "): "
				+ sinkNode.getLocalStrategy());
		}
		
		return sinkVertex;
	}

	/**
	 * @param stubClass
	 * @param dop
	 * @return
	 */
	private JobTaskVertex generateTempVertex(Class<?> stubClass, int dop, int instancesPerMachine) {
		// create task vertex
		JobTaskVertex tempVertex = new JobTaskVertex("TempVertex", this.jobGraph);
		// set task class
		tempVertex.setTaskClass(TempTask.class);

		// get task configuration object
		TaskConfig tempConfig = new TaskConfig(tempVertex.getConfiguration());
		// set key and value classes
		tempConfig.setStubClass(stubClass);

		assignMemory(tempConfig, PactCompiler.DEFAULT_TEMP_TASK_MEMORY);

		// set degree of parallelism
		tempVertex.setNumberOfSubtasks(dop);
		tempVertex.setNumberOfSubtasksPerInstance(instancesPerMachine);

		return tempVertex;
	}

	// ------------------------------------------------------------------------
	// Connecting Vertices
	// ------------------------------------------------------------------------

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 */
	private void connectWithForwardStrategy(PactConnection connection, int inputNumber,
			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
	throws CompilerException, JobGraphDefinitionException
	{
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Map:		// ok (Default)
		case Reduce:	// ok (Partitioning exists already)
		case Match: 	// ok (Partitioning exist already or forward for broadcast)
		case Cross:		// ok (Forward for non-broadcasted  side)
		case Cogroup:	// ok (Partitioning exist already)
		case DataSink:	// ok
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		connectJobVertices(connection, inputNumber, outputVertex, outputConfig, inputVertex, inputConfig);
	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithPartitionStrategy(PactConnection connection, int inputNumber,
			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
	throws CompilerException, JobGraphDefinitionException
	{
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType())
		{
		case Map:		// ok (Partitioning before map increases data volume)
		case Reduce:	// ok (Default)
		case Match:		// ok (Partitioning exist already or forward for broadcast)
		case Cross:		// ok (Partitioning with broadcast before cross increases data volume)
		case Cogroup:	// ok (Default)
		case DataSink:	// ok (Range partitioning for Global Sort)
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}
		connectJobVertices(connection, inputNumber, outputVertex, outputConfig, inputVertex, inputConfig);
	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithBroadcastStrategy(PactConnection connection, int inputNumber,
			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
	throws CompilerException, JobGraphDefinitionException
	{
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Match:		// ok (Broadcast)
		case Cross:		// ok (Broadcast)
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		connectJobVertices(connection, inputNumber, outputVertex, outputConfig, inputVertex, inputConfig);
	}

	/**
	 * @param connection
	 * @param inputNumber 
	 * @param outputVertex
	 * @param outputConfig 
	 * @param inputVertex
	 * @param inputConfig 
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithSFRStrategy(PactConnection connection, int inputNumber,
			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
	throws CompilerException, JobGraphDefinitionException
	{
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Cross: 	// ok
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		// TODO: implement SFR
		throw new UnsupportedOperationException("SFR shipping strategy not supported yet");
	}
	
//	/**
//	 * Adds the necessary vertexes for sampling & histogram creation etc for range partitioning
//	 * @param connection
//	 * @param outputVertex
//	 * @param inputVertex
//	 * @throws JobGraphDefinitionException 
//	 */
//	private void connectWithSamplingPartitionRangeStrategy(PactConnection connection, int inputNumber,
//			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
//			final AbstractJobVertex inputVertex, final TaskConfig inputConfig, boolean firstCall)
//	throws JobGraphDefinitionException
//	{
//		int sourceDOP = connection.getSourcePact().getDegreeOfParallelism();
//		int sourceIPM = connection.getSourcePact().getInstancesPerMachine();
//		int targetDOP = connection.getTargetPact().getDegreeOfParallelism();
////		int targetIPM = connection.getTargetPact().getInstancesPerMachine();
//		Class<?> sourceStub = connection.getSourcePact().getPactContract().getUserCodeClass();
//		
//		//TODO: Check for which pact types it makes sense
//		
//		//When parallelism is one there is nothing to partition
//		//if(sourceDOP == 1 && targetDOP == 1) {
//		if(targetDOP == 1) {
//			if(sourceDOP == 1) {
//				outputVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//			} else {
//				outputVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//			}
//			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//			inputConfig.addInputShipStrategy(ShipStrategy.FORWARD, inputNumber);
//			return;
//		}
//
//		
//		//Add sample vertex
//		JobTaskVertex sampleVertex = new JobTaskVertex("Range partition - sampling", this.jobGraph);
//		this.auxVertices.add(sampleVertex);
//		sampleVertex.setTaskClass(SampleTask.class);
//		TaskConfig sampleConfig = new TaskConfig(sampleVertex.getConfiguration());
//		//sampleConfig.setStubClass((Class<? extends Stub<?, ?>>) AdaptiveKeySampleStub.class);
//		sampleVertex.setNumberOfSubtasks(sourceDOP);
//		if (sourceIPM >= 1) {
//			sampleVertex.setNumberOfSubtasksPerInstance(sourceIPM);
//		}
//		sampleConfig.setStubClass(sourceStub);
//		//Connect with input
//		outputVertex.connectTo(sampleVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		sampleConfig.addInputShipStrategy(ShipStrategy.FORWARD, 1);
//		
//		
//		// firstCall indicated that this is the first call for one input which might have multiple predecessors
//		
//		// on the first call we have to instantiate the histogram which will be use by all predecessors
//		// -> on all consecutive calls we connect the new output node to the already instantiated histogramVertex
//		TaskConfig histogramConfig;
//		if(firstCall) {
//			//Add histogram building vertex;
//			this.histogramVertex = new JobTaskVertex("Range partition - histograming", this.jobGraph);
//			this.auxVertices.add(this.histogramVertex);
//			this.histogramVertex.setTaskClass(HistogramTask.class);
//			this.histogramVertex.setNumberOfSubtasks(1);
//			// reset counter because we instantiated a new histogram
//			this.numberOfHistogramInputs = 0;
//			
//			histogramConfig = new TaskConfig(this.histogramVertex.getConfiguration());
//			histogramConfig.setStubClass(sourceStub);
//			histogramConfig.setLocalStrategy(LocalStrategy.SORT);
//			Configuration histogramStubConfig = new Configuration();
//			histogramStubConfig.setInteger(HistogramTask.NUMBER_OF_BUCKETS, targetDOP);
//			histogramConfig.setStubParameters(histogramStubConfig);
//			assignMemory(histogramConfig, outputConfig.getStubParameters().getInteger(HistogramTask.HISTOGRAM_MEMORY,-1));
//		} else {
//			histogramConfig = new TaskConfig(this.histogramVertex.getConfiguration());
//		}
//		//Connect with input
//		histogramConfig.addInputShipStrategy(ShipStrategy.FORWARD, ++this.numberOfHistogramInputs);
//		sampleConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		sampleVertex.connectTo(this.histogramVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//		
//		
//		//Add range distributor vertex
//		JobTaskVertex partitionVertex = new JobTaskVertex("Range partition - partitioning", this.jobGraph);
//		this.auxVertices.add(partitionVertex);
//		partitionVertex.setTaskClass(PartitionTask.class);
//		partitionVertex.setNumberOfSubtasks(sourceDOP);
//		if (sourceIPM >= 1) {
//			partitionVertex.setNumberOfSubtasksPerInstance(sourceIPM);
//		}
//		TaskConfig partitionConfig = new TaskConfig(partitionVertex.getConfiguration());
//		partitionConfig.setStubClass(sourceStub);
//		Configuration partitionStubConfig = new Configuration();
//		partitionStubConfig.setString(PartitionTask.GLOBAL_PARTITIONING_ORDER, 
//			connection.getTargetPact().getGlobalProperties().getOrdering().getOrder(0).name());
//		partitionConfig.setStubParameters(partitionStubConfig);
//			
//		//Add temp vertex to avoid blocking
//		JobTaskVertex tempVertex = generateTempVertex(
//				// source pact stub contains out key and value
//				connection.getSourcePact().getPactContract().getUserCodeClass(),
//				// keep parallelization of source pact
//				sourceDOP, sourceIPM);
//		
//		tempVertex.setVertexToShareInstancesWith(outputVertex);
//		TaskConfig tempConfig = new TaskConfig(tempVertex.getConfiguration());
//
//		//Connect data to tempVertex (partitioner)
//		outputVertex.connectTo(tempVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//		tempConfig.addInputShipStrategy(ShipStrategy.FORWARD, 1);
//		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		
//		//Connect tempVertex (data) to partitionVertex
//		tempVertex.connectTo(partitionVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//		// tempVertex is always connected as second input to the partitioner (2-input node).
//		// the second input is the data input
//		partitionConfig.addInputShipStrategy(ShipStrategy.FORWARD, 2);
//		tempConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//			
//		//Connect histogram with partitioner
//		this.histogramVertex.connectTo(partitionVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
//		// histogramVertex is always connected as first input to the partitioner (2-input node).
//		// the first input is the statistic input
//		partitionConfig.addInputShipStrategy(ShipStrategy.BROADCAST, 1);
//		histogramConfig.addOutputShipStrategy(ShipStrategy.BROADCAST);
//
//		//Connect to receiving vertex
//		partitionVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
//		inputConfig.addInputShipStrategy(ShipStrategy.PARTITION_RANGE, inputNumber);
//		partitionConfig.addOutputShipStrategy(ShipStrategy.PARTITION_RANGE);
//	}
//	
//	/**
//	 * Implements range partitioning with a user-defined data distribution
//	 * @param connection
//	 * @param outputVertex
//	 * @param inputVertex
//	 * @throws JobGraphDefinitionException 
//	 */
//	private void connectWithGivenDistributionPartitionRangeStrategy(PactConnection connection, int inputNumber,
//			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
//			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
//	throws JobGraphDefinitionException
//	{
//		int sourceDOP = connection.getSourcePact().getDegreeOfParallelism();
//		int sourceIPM = connection.getSourcePact().getInstancesPerMachine();
//		int targetDOP = connection.getTargetPact().getDegreeOfParallelism();
////		int targetIPM = connection.getTargetPact().getInstancesPerMachine();
//		Class<?> sourceStub = connection.getSourcePact().getPactContract().getUserCodeClass();
//		
//		//When parallelism is one there is nothing to partition
//		//if(sourceDOP == 1 && targetDOP == 1) {
//		if(targetDOP == 1) {
//			if(sourceDOP == 1) {
//				outputVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//			} else {
//				outputVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//			}
//			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//			inputConfig.addInputShipStrategy(ShipStrategy.FORWARD, inputNumber);
//			return;
//		}
//		
//		//Add range distributor vertex
//		JobTaskVertex partitionVertex = new JobTaskVertex("Range partition - partitioning", this.jobGraph);
//		this.auxVertices.add(partitionVertex);
//		partitionVertex.setTaskClass(PartitionTask.class);
//		partitionVertex.setNumberOfSubtasks(sourceDOP);
//		if (sourceIPM >= 1) {
//			partitionVertex.setNumberOfSubtasksPerInstance(sourceIPM);
//		}
//		TaskConfig partitionConfig = new TaskConfig(partitionVertex.getConfiguration());
//		partitionConfig.setStubClass(sourceStub);
//		Configuration partitionStubConfig = new Configuration();
//		partitionStubConfig.setString(PartitionTask.GLOBAL_PARTITIONING_ORDER, 
//			connection.getTargetPact().getGlobalProperties().getOrdering().getOrder(0).name());
//		partitionStubConfig.setBoolean(PartitionTask.PARTITION_BY_SAMPLING, false);
//		partitionStubConfig.setInteger(PartitionTask.NUMBER_OF_PARTITIONS, targetDOP);
//		partitionStubConfig.setClass(PartitionTask.DATA_DISTRIBUTION_CLASS,
//			connection.getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass());
//		partitionConfig.setStubParameters(partitionStubConfig);
//		
//		//Connect partitioner with sending vertex
//		outputVertex.connectTo(partitionVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
//		// we don't use the statistic input, because the partitioning strategy is given
//		// hence partitionVertex in now a single input node, hence we connect to input 1
//		partitionConfig.addInputShipStrategy(ShipStrategy.FORWARD, 1);
//		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
//		
//		//Connect to receiving vertex
//		partitionVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION, DistributionPattern.BIPARTITE);
//		inputConfig.addInputShipStrategy(ShipStrategy.PARTITION_RANGE, inputNumber);
//		partitionConfig.addOutputShipStrategy(ShipStrategy.PARTITION_RANGE);
//	}


	/**
	 * @param connection
	 * @param inputNumber
	 * @param outputVertex
	 * @param outputConfig
	 * @param inputVertex
	 * @param inputConfig
	 * @throws JobGraphDefinitionException
	 * @throws CompilerException
	 */
	@SuppressWarnings("unchecked")
	private void connectJobVertices(PactConnection connection, int inputNumber,
			final AbstractJobVertex outputVertex, final TaskConfig outputConfig,
			final AbstractJobVertex inputVertex, final TaskConfig inputConfig)
	throws JobGraphDefinitionException, CompilerException
	{
		final ChannelType channelType;
		final DistributionPattern distributionPattern;

		switch (connection.getShipStrategy()) {
		case FORWARD:
		case PARTITION_LOCAL_HASH:
			int sourceDOP = connection.getSourcePact().getDegreeOfParallelism();
			int sourceInnerDOP = connection.getSourcePact().getInstancesPerMachine();
			int sourceNumInstances = (int) Math.ceil((double) sourceDOP / (double) sourceInnerDOP);
			
			int targetDOP = connection.getTargetPact().getDegreeOfParallelism();
			int targetInnerDOP = connection.getTargetPact().getInstancesPerMachine();
			int targetNumInstances = (int) Math.ceil((double) targetDOP / (double) targetInnerDOP);
			
			channelType = sourceNumInstances == targetNumInstances ? ChannelType.INMEMORY : ChannelType.NETWORK;
			distributionPattern = DistributionPattern.POINTWISE;
			break;
		case PARTITION_RANGE:
		case PARTITION_HASH:
		case BROADCAST:
			channelType = ChannelType.NETWORK;
			distributionPattern = DistributionPattern.BIPARTITE;
			break;
		default:
			throw new IllegalArgumentException("Unsupported ship-strategy: " + connection.getShipStrategy().name());
		}

		final TaskConfig tempConfig;
		
		final int[] keyPositions;
		final Class<? extends Key>[] keyTypes;
		
		final Contract targetContract = connection.getTargetPact().getPactContract();
		if (targetContract instanceof AbstractPact<?>) {
			AbstractPact<?> pact = (AbstractPact<?>) targetContract;
			if (connection.getScramblePartitionedFields() != null) {
				int[] originalKeyPositions = pact.getKeyColumnNumbers(inputNumber-1);
				Class<? extends Key>[] originalKeyTypes = pact.getKeyClasses();
				int [] scrambleArray = connection.getScramblePartitionedFields();
				keyTypes = new Class[scrambleArray.length];
				keyPositions = new int[scrambleArray.length];
				
				for (int i = 0; i < scrambleArray.length; i++) {
					keyPositions[i] = originalKeyPositions[scrambleArray[i]];
					keyTypes[i] = originalKeyTypes[scrambleArray[i]];
				}
			}
			else {
				keyPositions = pact.getKeyColumnNumbers(inputNumber-1);
				keyTypes = pact.getKeyClasses();	
			}
		} else if (targetContract instanceof GenericDataSink) {
			final Ordering o = ((GenericDataSink) targetContract).getPartitionOrdering();
			if (o != null) {
				final int numFields = o.getNumberOfFields();
				keyPositions = new int[numFields];
				keyTypes = new Class[numFields];
				for (int i = 0; i < numFields; i++) {
					keyPositions[i] = o.getFieldNumber(i);
					keyTypes[i] = o.getType(i);
				}
			} else {
				keyPositions = null;
				keyTypes = null;
			}
		} else {
			keyPositions = null;
			keyTypes = null;
		}

		final TaskConfig configForOutputShipStrategy;
		switch (connection.getTempMode()) {
		case NONE:
			outputVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
			configForOutputShipStrategy = outputConfig;
			inputConfig.addInputToGroup(inputNumber);
			break;
		case TEMP_SENDER_SIDE:
			// create tempTask
			int degreeOfParallelism = connection.getSourcePact().getDegreeOfParallelism();
			int instancesPerMachine = connection.getSourcePact().getInstancesPerMachine();

			JobTaskVertex tempVertex = generateTempVertex(
			// source pact stub contains out key and value
				connection.getSourcePact().getPactContract().getUserCodeClass(),
				// keep parallelization of source pact
				degreeOfParallelism, instancesPerMachine);

			// insert tempVertex between outputVertex and inputVertex and connect them
			outputVertex.connectTo(tempVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);
			tempVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);

			tempVertex.setVertexToShareInstancesWith(outputVertex);
			
			// get tempVertex config
			tempConfig = new TaskConfig(tempVertex.getConfiguration());

			// set strategies in task configs
			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			configForOutputShipStrategy = tempConfig;
			inputConfig.addInputToGroup(inputNumber);
			tempConfig.addInputToGroup(1);

			break;
		case TEMP_RECEIVER_SIDE:
			degreeOfParallelism = connection.getTargetPact().getDegreeOfParallelism();
			instancesPerMachine = connection.getTargetPact().getInstancesPerMachine();

			// create tempVertex
			tempVertex = generateTempVertex(
			// source pact stub contains out key and value
				connection.getSourcePact().getPactContract().getUserCodeClass(),
				// keep parallelization of target pact
				degreeOfParallelism, instancesPerMachine);

			// insert tempVertex between outputVertex and inputVertex and connect them
			outputVertex.connectTo(tempVertex, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
			tempVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION, DistributionPattern.POINTWISE);

			tempVertex.setVertexToShareInstancesWith(inputVertex);
			
			// get tempVertex config
			tempConfig = new TaskConfig(tempVertex.getConfiguration());

			// set strategies in task configs
			tempConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			configForOutputShipStrategy = outputConfig;
			
			inputConfig.addInputToGroup(inputNumber);
			tempConfig.addInputToGroup(1);
			break;
		default:
			throw new CompilerException("Invalid connection temp mode: " + connection.getTempMode());
		}
		
		// set strategies in task configs
		configForOutputShipStrategy.addOutputShipStrategy(connection.getShipStrategy());
		if (! (keyPositions == null || keyTypes == null || keyPositions.length == 0 || keyTypes.length == 0))
		{
			final int outputNum = configForOutputShipStrategy.getNumOutputs() - 1;
			configForOutputShipStrategy.setComparatorFactoryForOutput(PactRecordComparatorFactory.class, outputNum);
			PactRecordComparatorFactory.writeComparatorSetupToConfig(configForOutputShipStrategy.getConfiguration(),
				configForOutputShipStrategy.getPrefixForOutputParameters(outputNum), keyPositions, keyTypes);
		}
		
		if (targetContract instanceof GenericDataSink) {
			final DataDistribution distri = ((GenericDataSink) targetContract).getDataDistribution();
			if (distri != null) {
				configForOutputShipStrategy.setOutputDataDistribution(distri);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Assigning Memory
	// ------------------------------------------------------------------------

	private void assignMemory(TaskConfig config, int memSize)
	{
		config.setMemorySize(memSize * 1024L * 1024L);
		config.setNumFilehandles(DEFAULT_MERGE_FACTOR);
	}
	
	// ------------------------------------------------------------------------
	// Task Chaining
	// ------------------------------------------------------------------------
	
	private static boolean isChainable(OptimizerNode node)
	{
		// node needs to have one input and be the only successor of its predecessor
		if (node.getIncomingConnections().size() == 1) {
			final PactConnection inConn = node.getIncomingConnections().get(0);
			
			final OptimizerNode predecessor = inConn.getSourcePact();
			if (inConn.getShipStrategy() == ShipStrategy.FORWARD && predecessor.getOutConns().size() == 1) {
				return node.getDegreeOfParallelism() == predecessor.getDegreeOfParallelism() && 
						node.getInstancesPerMachine() == predecessor.getInstancesPerMachine();
			}
		}
		
		return false;
	}
	
	private static final class TaskInChain
	{
		private final Class<? extends ChainedTask<?, ?>> chainedTask;
		
		private final TaskConfig taskConfig;
		
		private final String taskName;
		
		private AbstractJobVertex containingVertex;

		@SuppressWarnings("unchecked")
		TaskInChain(@SuppressWarnings("rawtypes") Class<? extends ChainedTask> chainedTask, TaskConfig taskConfig, String taskName) {
			this.chainedTask = (Class<? extends ChainedTask<?, ?>>) chainedTask;
			this.taskConfig = taskConfig;
			this.taskName = taskName;
		}
		
		public Class<? extends ChainedTask<?, ?>> getChainedTask() {
			return this.chainedTask;
		}
		
		public TaskConfig getTaskConfig() {
			return this.taskConfig;
		}
		
		public String getTaskName() {
			return this.taskName;
		}
		
		public AbstractJobVertex getContainingVertex() {
			return this.containingVertex;
		}
		
		public void setContainingVertex(AbstractJobVertex containingVertex) {
			this.containingVertex = containingVertex;
		}
	}
}
