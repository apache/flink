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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.AbstractJobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.plan.CombinerNode;
import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.compiler.plan.DataSourceNode;
import eu.stratosphere.pact.compiler.plan.MapNode;
import eu.stratosphere.pact.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.compiler.plan.ReduceNode;
import eu.stratosphere.pact.runtime.task.CoGroupTask;
import eu.stratosphere.pact.runtime.task.CombineTask;
import eu.stratosphere.pact.runtime.task.CrossTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.HistogramTask;
import eu.stratosphere.pact.runtime.task.MapTask;
import eu.stratosphere.pact.runtime.task.MatchTask;
import eu.stratosphere.pact.runtime.task.PartitionTask;
import eu.stratosphere.pact.runtime.task.ReduceTask;
import eu.stratosphere.pact.runtime.task.SampleTask;
import eu.stratosphere.pact.runtime.task.SelfMatchTask;
import eu.stratosphere.pact.runtime.task.TempTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
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
	
	public static final Log LOG = LogFactory.getLog(JobGraphGenerator.class);
	
	private static final int DEFAULT_MERGE_FACTOR = 64; // the number of streams to merge at once
	
	// ------------------------------------------------------------------------

	private JobGraph jobGraph; // the job that is currently built

	private Map<OptimizerNode, AbstractJobVertex> vertices; // a map from optimizer nodes to nephele vertices
	
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
	public JobGraph compileJobGraph(OptimizedPlan pactPlan) {
		this.jobGraph = new JobGraph(pactPlan.getJobName());
		this.vertices = new HashMap<OptimizerNode, AbstractJobVertex>();
		this.auxVertices = new ArrayList<AbstractJobVertex>();
		this.maxDegreeVertex = null;
		
		// set Nephele JobGraph config
		pactPlan.getPlanConfiguration().extractNepheleConfiguration(this.jobGraph.getJobConfiguration());
		
		// generate Nephele job graph
		pactPlan.accept(this);

		// now that all have been created, make sure that all share their instances with the one
		// with the highest degree of parallelism
		if (pactPlan.getInstanceTypeName() != null) {
			maxDegreeVertex.setInstanceType(pactPlan.getInstanceTypeName());
		} else {
			LOG.warn("No instance type assigned to Nephele JobVertex.");
		}
		for (AbstractJobVertex vertex : this.vertices.values()) {
			if (vertex == maxDegreeVertex) {
				continue;
			}
			vertex.setVertexToShareInstancesWith(maxDegreeVertex);
		}
		
		for (AbstractJobVertex vertex : this.auxVertices) {
			if (vertex == maxDegreeVertex) {
				continue;
			}
			vertex.setVertexToShareInstancesWith(maxDegreeVertex);
		}
		

		JobGraph graph = this.jobGraph;

		// release all references again
		this.maxDegreeVertex = null;
		this.vertices = null;
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
	public boolean preVisit(OptimizerNode node) {
		// check if we have visited this node before. in non-tree graphs, this happens
		if (vertices.containsKey(node)) {
			return false;
		}

		// the vertex to be created for the current node
		AbstractJobVertex vertex = null;

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
			default:
				throw new Exception("Unknown PACT type: " + node.getPactType());
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}

		// set degree of parallelism
		int pd = node.getDegreeOfParallelism();
		vertex.setNumberOfSubtasks(pd);

		// check whether this is the vertex with the highest degree of parallelism
		if (maxDegreeVertex == null || maxDegreeVertex.getNumberOfSubtasks() < pd) {
			maxDegreeVertex = vertex;
		}

		// set the number of tasks per instance
		if (node.getInstancesPerMachine() >= 1) {
			vertex.setNumberOfSubtasksPerInstance(node.getInstancesPerMachine());
		}

		// store in the map
		this.vertices.put(node, vertex);

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
	public void postVisit(OptimizerNode node) {

		try {
			// get pact vertex
			AbstractJobVertex inputVertex = this.vertices.get(node);
			List<PactConnection> incomingConns = node.getIncomingConnections();

			if (incomingConns == null) {
				// data source
				return;
			}

			for (PactConnection connection : node.getIncomingConnections()) {
				// get parent vertex
				AbstractJobVertex outputVertex = this.vertices.get(connection.getSourcePact());
				if (outputVertex == null) {
					throw new Exception("Parent vertex was not initialized");
				}

				switch (connection.getShipStrategy()) {
				case FORWARD:
					connectWithForwardStrategy(connection, outputVertex, inputVertex);
					break;
				case PARTITION_LOCAL_HASH:
				case PARTITION_HASH:
					connectWithPartitionStrategy(connection, outputVertex, inputVertex);
					break;
				case BROADCAST:
					connectWithBroadcastStrategy(connection, outputVertex, inputVertex);
					break;
				case PARTITION_RANGE:
					if(isDistributionGiven(connection)) {
						connectWithGivenDistributionPartitionRangeStrategy(connection, outputVertex, inputVertex);
					} else {
						connectWithSamplingPartitionRangeStrategy(connection, outputVertex, inputVertex);
					}
					break;
				case SFR:
					connectWithSFRStrategy(connection, outputVertex, inputVertex);
				default:
					throw new Exception("Invalid ship strategy: " + connection.getShipStrategy());
				}
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}
	}
	
	// ------------------------------------------------------------------------
	// Methods for creating individual vertices
	// ------------------------------------------------------------------------

	private boolean isDistributionGiven(PactConnection connection) {
		return (connection.getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass() != null);
	}
	
	/**
	 * @param mapNode
	 * @return
	 * @throws CompilerException
	 */
	private JobTaskVertex generateMapVertex(OptimizerNode mapNode) throws CompilerException
	{
		MapContract mc = ((MapNode) mapNode).getPactContract();
		
		// create task vertex
		JobTaskVertex mapVertex = new JobTaskVertex(mapNode.getPactContract().getName(), this.jobGraph);
		// set task class
		mapVertex.setTaskClass(MapTask.class);

		// get task configuration object
		TaskConfig mapConfig = new TaskConfig(mapVertex.getConfiguration());
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
	private JobTaskVertex generateCombineVertex(CombinerNode combineNode) throws CompilerException {
		JobTaskVertex combineVertex = new JobTaskVertex("Combiner for " + combineNode.getPactContract().getName(),
			this.jobGraph);
		combineVertex.setTaskClass(CombineTask.class);

		TaskConfig combineConfig = new TaskConfig(combineVertex.getConfiguration());
		combineConfig.setStubClass(combineNode.getPactContract().getUserCodeClass());

		// we have currently only one strategy for combiners
		combineConfig.setLocalStrategy(LocalStrategy.COMBININGSORT);

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
	private JobTaskVertex generateReduceVertex(ReduceNode reduceNode) throws CompilerException {
		// create task vertex
		JobTaskVertex reduceVertex = new JobTaskVertex(reduceNode.getPactContract().getName(), this.jobGraph);
		// set task class
		reduceVertex.setTaskClass(ReduceTask.class);

		// get task configuration object
		TaskConfig reduceConfig = new TaskConfig(reduceVertex.getConfiguration());
		// set user code class
		reduceConfig.setStubClass(reduceNode.getPactContract().getUserCodeClass());

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
	private JobTaskVertex generateMatchVertex(OptimizerNode matchNode) throws CompilerException {
		// create task vertex
		JobTaskVertex matchVertex = new JobTaskVertex(matchNode.getPactContract().getName(), this.jobGraph);

		// get task configuration object
		TaskConfig matchConfig = new TaskConfig(matchVertex.getConfiguration());
		// set user code class
		matchConfig.setStubClass(matchNode.getPactContract().getUserCodeClass());

		switch (matchNode.getLocalStrategy()) {
		case SORT_BOTH_MERGE:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			break;
		case SORT_FIRST_MERGE:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			break;
		case SORT_SECOND_MERGE:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			break;
		case MERGE:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.MERGE);
			break;
		case HYBRIDHASH_FIRST:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
			break;
		case HYBRIDHASH_SECOND:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
			break;
		case MMHASH_FIRST:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.MMHASH_FIRST);
			break;
		case MMHASH_SECOND:
			// set task class
			matchVertex.setTaskClass(MatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.MMHASH_SECOND);
			break;
		case SORT_SELF_NESTEDLOOP:
			// set task class
			matchVertex.setTaskClass(SelfMatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
			break;
		case SELF_NESTEDLOOP:
			// set task class
			matchVertex.setTaskClass(SelfMatchTask.class);
			// set local strategy
			matchConfig.setLocalStrategy(LocalStrategy.SELF_NESTEDLOOP);
			break;
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
		// create task vertex
		JobTaskVertex coGroupVertex = new JobTaskVertex(coGroupNode.getPactContract().getName(), this.jobGraph);
		// set task class
		coGroupVertex.setTaskClass(CoGroupTask.class);

		// get task configuration object
		TaskConfig coGroupConfig = new TaskConfig(coGroupVertex.getConfiguration());
		// set user code class
		coGroupConfig.setStubClass(coGroupNode.getPactContract().getUserCodeClass());

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
		sourceVertex.setInputClass(DataSourceTask.class);

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
		// set the sort order into config (can also be NONE)
		sinkVertex.getConfiguration().setString(DataSinkTask.SORT_ORDER, sinkNode.getLocalProperties().getKeyOrder().name());
		// get task configuration object
		TaskConfig sinkConfig = new TaskConfig(sinkVertex.getConfiguration());
		// set user code class
		sinkConfig.setStubClass(sinkContract.getUserCodeClass());
		// forward stub parameters to task and data format
		sinkConfig.setStubParameters(sinkContract.getParameters());

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

		//HACK: Copied from Reduce task, is memory always assigned even if not needed?
		//		could be same problem in reduce task
		assignMemory(sinkConfig, sinkNode.getMemoryPerTask());
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
	private void connectWithForwardStrategy(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws CompilerException, JobGraphDefinitionException {
		// TODO: currently we do a 1-to-1 mapping one the same instance. Hence, we use INMEMORY channels
		// We should add the possibility to distribute the load to multiple machines (one local, x remote)

		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Map:
			// ok (Default)
			break;
		case Reduce:
			// ok (Partitioning exists already)
			break;
		case Match:
			// ok (Partitioning exist already or forward for broadcast)
			break;
		case Cross:
			// ok (Forward for broadcast)
			break;
		case Cogroup:
			// ok (Partitioning exist already)
			break;
		case DataSink:
			// ok
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		connectJobVertices(connection, outputVertex, inputVertex);

	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithPartitionStrategy(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws CompilerException, JobGraphDefinitionException {
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Map:
			// ok (Partitioning before map increases data volume)
			break;
		case Reduce:
			// ok (Default)
			break;
		case Match:
			// ok (Partitioning exist already or forward for broadcast)
			break;
		case Cross:
			// ok (Partitioning with broadcast before cross increases data volume)
			break;
		case Cogroup:
			// ok (Default)
			break;
		case DataSink:
			// ok
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		connectJobVertices(connection, outputVertex, inputVertex);
	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithBroadcastStrategy(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws CompilerException, JobGraphDefinitionException {
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Match:
			// ok (Broadcast)
			break;
		case Cross:
			// ok (Broadcast)
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		connectJobVertices(connection, outputVertex, inputVertex);
	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws CompilerException
	 * @throws JobGraphDefinitionException
	 */
	private void connectWithSFRStrategy(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws CompilerException, JobGraphDefinitionException {
		// check if shipStrategy suits child
		switch (connection.getTargetPact().getPactType()) {
		case Cross:
			// ok
			break;
		default:
			throw new CompilerException("ShipStrategy " + connection.getShipStrategy().name() + " does not suit PACT "
				+ connection.getTargetPact().getPactType().name());
		}

		// TODO: implement SFR
		throw new UnsupportedOperationException("SFR shipping strategy not supported yet");
	}
	
	/**
	 * Adds the necessary vertexes for sampling & histogram creation etc for range partitioning
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws JobGraphDefinitionException 
	 */
	private void connectWithSamplingPartitionRangeStrategy(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws JobGraphDefinitionException {
		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		TaskConfig outputConfig = new TaskConfig(outputVertex.getConfiguration());
		int sourceDOP = connection.getSourcePact().getDegreeOfParallelism();
		int sourceIPM = connection.getSourcePact().getInstancesPerMachine();
		int targetDOP = connection.getTargetPact().getDegreeOfParallelism();
		int targetIPM = connection.getTargetPact().getInstancesPerMachine();
		Class<?> sourceStub = connection.getSourcePact().getPactContract().getUserCodeClass();
		
		//TODO: Check for which pact types it makes sense
		
		//When parallelism is one there is nothing to partition
		//if(sourceDOP == 1 && targetDOP == 1) {
		if(targetDOP == 1) {
			if(sourceDOP == 1) {
				outputVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} else {
				outputVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			}
			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			inputConfig.addInputShipStrategy(ShipStrategy.FORWARD);
			return;
		}
		
		//Add sample vertex
		JobTaskVertex sampleVertex = new JobTaskVertex("Range partition - sampling", this.jobGraph);
		auxVertices.add(sampleVertex);
		sampleVertex.setTaskClass(SampleTask.class);
		TaskConfig sampleConfig = new TaskConfig(sampleVertex.getConfiguration());
		//sampleConfig.setStubClass((Class<? extends Stub<?, ?>>) AdaptiveKeySampleStub.class);
		sampleVertex.setNumberOfSubtasks(sourceDOP);
		if (sourceIPM >= 1) {
			sampleVertex.setNumberOfSubtasksPerInstance(sourceIPM);
		}
		sampleConfig.setStubClass(sourceStub);
		//Connect with input
		outputVertex.connectTo(sampleVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
		sampleConfig.addInputShipStrategy(ShipStrategy.FORWARD);
		
		
		//Add histogram building vertex;
		JobTaskVertex histogramVertex = new JobTaskVertex("Range partition - histograming", this.jobGraph);
		auxVertices.add(histogramVertex);
		histogramVertex.setTaskClass(HistogramTask.class);
		histogramVertex.setNumberOfSubtasks(1);
		TaskConfig histogramConfig = new TaskConfig(histogramVertex.getConfiguration());
		histogramConfig.setStubClass(sourceStub);
		histogramConfig.setLocalStrategy(LocalStrategy.SORT);
		Configuration histogramStubConfig = new Configuration();
		histogramStubConfig.setInteger(HistogramTask.NUMBER_OF_BUCKETS, targetDOP);
		histogramConfig.setStubParameters(histogramStubConfig);
		assignMemory(histogramConfig, outputConfig.getStubParameters().getInteger(HistogramTask.HISTOGRAM_MEMORY,-1));
		//Connect with input
		histogramConfig.addInputShipStrategy(ShipStrategy.FORWARD);
		sampleConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
		sampleVertex.connectTo(histogramVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
		
		//Add range distributor vertex
		JobTaskVertex partitionVertex = new JobTaskVertex("Range partition - partitioning", this.jobGraph);
		auxVertices.add(partitionVertex);
		partitionVertex.setTaskClass(PartitionTask.class);
		partitionVertex.setNumberOfSubtasks(sourceDOP);
		if (sourceIPM >= 1) {
			partitionVertex.setNumberOfSubtasksPerInstance(sourceIPM);
		}
		TaskConfig partitionConfig = new TaskConfig(partitionVertex.getConfiguration());
		partitionConfig.setStubClass(sourceStub);
		Configuration partitionStubConfig = new Configuration();
		partitionStubConfig.setString(PartitionTask.GLOBAL_PARTITIONING_ORDER, 
			connection.getTargetPact().getGlobalProperties().getKeyOrder().name());
		partitionConfig.setStubParameters(partitionStubConfig);
		
		//Add temp vertex to avoid blocking
		JobTaskVertex tempVertex = generateTempVertex(
				// source pact stub contains out key and value
				connection.getSourcePact().getPactContract().getUserCodeClass(),
				// keep parallelization of source pact
				sourceDOP, sourceIPM);
		
		tempVertex.setVertexToShareInstancesWith(outputVertex);
		TaskConfig tempConfig = new TaskConfig(tempVertex.getConfiguration());

		//Connect data to tempVertex (partitioner)
		outputVertex.connectTo(tempVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		tempConfig.addInputShipStrategy(ShipStrategy.FORWARD);
		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
		
		//Connect tempVertex (data) to partitionVertex
		tempVertex.connectTo(partitionVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		partitionConfig.addInputShipStrategy(ShipStrategy.FORWARD);
		tempConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
		
		//Connect histogram with partitioner
		histogramVertex.connectTo(partitionVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
		partitionConfig.addInputShipStrategy(ShipStrategy.BROADCAST);
		histogramConfig.addOutputShipStrategy(ShipStrategy.BROADCAST);

		//Connect to receiving vertex
		partitionVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
		inputConfig.addInputShipStrategy(ShipStrategy.PARTITION_RANGE);
		partitionConfig.addOutputShipStrategy(ShipStrategy.PARTITION_RANGE);
	}
	
	/**
	 * Implements range partitioning with a user-defined data distribution
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws JobGraphDefinitionException 
	 */
	private void connectWithGivenDistributionPartitionRangeStrategy(PactConnection connection,
			AbstractJobVertex outputVertex, AbstractJobVertex inputVertex) throws JobGraphDefinitionException {
		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		TaskConfig outputConfig = new TaskConfig(outputVertex.getConfiguration());
		int sourceDOP = connection.getSourcePact().getDegreeOfParallelism();
		int sourceIPM = connection.getSourcePact().getInstancesPerMachine();
		int targetDOP = connection.getTargetPact().getDegreeOfParallelism();
		int targetIPM = connection.getTargetPact().getInstancesPerMachine();
		Class<?> sourceStub = connection.getSourcePact().getPactContract().getUserCodeClass();
		
		//When parallelism is one there is nothing to partition
		//if(sourceDOP == 1 && targetDOP == 1) {
		if(targetDOP == 1) {
			if(sourceDOP == 1) {
				outputVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			} else {
				outputVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
			}
			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			inputConfig.addInputShipStrategy(ShipStrategy.FORWARD);
			return;
		}
		
		//Add range distributor vertex
		JobTaskVertex partitionVertex = new JobTaskVertex("Range partition - partitioning", this.jobGraph);
		auxVertices.add(partitionVertex);
		partitionVertex.setTaskClass(PartitionTask.class);
		partitionVertex.setNumberOfSubtasks(sourceDOP);
		if (sourceIPM >= 1) {
			partitionVertex.setNumberOfSubtasksPerInstance(sourceIPM);
		}
		TaskConfig partitionConfig = new TaskConfig(partitionVertex.getConfiguration());
		partitionConfig.setStubClass(sourceStub);
		Configuration partitionStubConfig = new Configuration();
		partitionStubConfig.setString(PartitionTask.GLOBAL_PARTITIONING_ORDER, 
			connection.getTargetPact().getGlobalProperties().getKeyOrder().name());
		partitionStubConfig.setBoolean(PartitionTask.PARTITION_BY_SAMPLING, false);
		partitionStubConfig.setInteger(PartitionTask.NUMBER_OF_PARTITIONS, targetDOP);
		partitionStubConfig.setClass(PartitionTask.DATA_DISTRIBUTION_CLASS,
			connection.getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass());
		partitionConfig.setStubParameters(partitionStubConfig);
		
		//Connect partitioner with sending vertex
		outputVertex.connectTo(partitionVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
		partitionConfig.addInputShipStrategy(ShipStrategy.FORWARD);
		outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
		
		//Connect to receiving vertex
		partitionVertex.connectTo(inputVertex, ChannelType.NETWORK, CompressionLevel.NO_COMPRESSION);
		inputConfig.addInputShipStrategy(ShipStrategy.PARTITION_RANGE);
		partitionConfig.addOutputShipStrategy(ShipStrategy.PARTITION_RANGE);
	}

	/**
	 * @param connection
	 * @param outputVertex
	 * @param inputVertex
	 * @throws JobGraphDefinitionException
	 * @throws CompilerException
	 */
	private void connectJobVertices(PactConnection connection, AbstractJobVertex outputVertex,
			AbstractJobVertex inputVertex) throws JobGraphDefinitionException, CompilerException {
		ChannelType channelType = null;

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
			break;
		case PARTITION_HASH:
		case BROADCAST:
		case SFR:
			channelType = ChannelType.NETWORK;
			break;
		default:
			throw new IllegalArgumentException("Unsupported ship-strategy: " + connection.getShipStrategy().name());
		}

		TaskConfig outputConfig = new TaskConfig(outputVertex.getConfiguration());
		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		TaskConfig tempConfig = null;

		switch (connection.getTempMode()) {
		case NONE:
			// connect child with inmemory channel
			outputVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION);
			// set ship strategy in vertex and child

			// set strategies in task configs
			outputConfig.addOutputShipStrategy(connection.getShipStrategy());
			inputConfig.addInputShipStrategy(connection.getShipStrategy());
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
			outputVertex.connectTo(tempVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);
			tempVertex.connectTo(inputVertex, channelType, CompressionLevel.NO_COMPRESSION);

			tempVertex.setVertexToShareInstancesWith(outputVertex);
			
			// get tempVertex config
			tempConfig = new TaskConfig(tempVertex.getConfiguration());

			// set strategies in task configs
			outputConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			tempConfig.addInputShipStrategy(ShipStrategy.FORWARD);
			tempConfig.addOutputShipStrategy(connection.getShipStrategy());
			inputConfig.addInputShipStrategy(connection.getShipStrategy());

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
			outputVertex.connectTo(tempVertex, channelType, CompressionLevel.NO_COMPRESSION);
			tempVertex.connectTo(inputVertex, ChannelType.INMEMORY, CompressionLevel.NO_COMPRESSION);

			tempVertex.setVertexToShareInstancesWith(inputVertex);
			
			// get tempVertex config
			tempConfig = new TaskConfig(tempVertex.getConfiguration());

			// set strategies in task configs
			outputConfig.addOutputShipStrategy(connection.getShipStrategy());
			tempConfig.addInputShipStrategy(connection.getShipStrategy());
			tempConfig.addOutputShipStrategy(ShipStrategy.FORWARD);
			inputConfig.addInputShipStrategy(ShipStrategy.FORWARD);

			break;
		default:
			throw new CompilerException("Invalid connection temp mode: " + connection.getTempMode());
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

}
