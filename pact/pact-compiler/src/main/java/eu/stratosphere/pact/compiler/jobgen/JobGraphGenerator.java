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
import java.util.Iterator;
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
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.util.PactConfigConstants;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SourcePlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.UnionPlanNode;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.chaining.ChainedDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapDriver;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * This component translates the optimizer's resulting plan a nephele job graph. The
 * translation is a one to one mapping. All decisions are made by the optimizer, this class
 * simply creates nephele data structures and descriptions corresponding to the optimizer's
 * result.
 */
public class JobGraphGenerator implements Visitor<PlanNode>
{	
	private static final Log LOG = LogFactory.getLog(JobGraphGenerator.class);
	
	// ------------------------------------------------------------------------

	private JobGraph jobGraph; // the job that is currently built

	private Map<PlanNode, AbstractJobVertex> vertices; // a map from optimizer nodes to nephele vertices
	
	private Map<PlanNode, TaskInChain> chainedTasks; // a map from optimizer nodes to nephele vertices
	
	private List<TaskInChain> chainedTasksInSequence;
	
	private List<AbstractJobVertex> auxVertices; // auxiliary vertices which are added during job graph generation

	private AbstractJobVertex maxDegreeVertex; // the vertex with the highest degree of parallelism
	
	private final int defaultMaxFan;
	
	private final float defaultSortSpillingThreshold;
	
	// ------------------------------------------------------------------------

	/**
	 * Creates a new job graph generator that uses the default values for its resource configuration.
	 */
	public JobGraphGenerator() {
		this.defaultMaxFan = PactConfigConstants.DEFAULT_SPILLING_MAX_FAN;
		this.defaultSortSpillingThreshold = PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD;
	}
	
	public JobGraphGenerator(Configuration config) {
		this.defaultMaxFan = config.getInteger(PactConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY, 
				PactConfigConstants.DEFAULT_SPILLING_MAX_FAN);
		this.defaultSortSpillingThreshold = config.getFloat(PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY,
			PactConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
	}

	/**
	 * Translates a {@link eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan} into a
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
		this.vertices = new HashMap<PlanNode, AbstractJobVertex>();
		this.chainedTasks = new HashMap<PlanNode, TaskInChain>();
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
			if (vertex != this.maxDegreeVertex) {
				vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
			}
		}
		
		for (AbstractJobVertex vertex : this.auxVertices) {
			if (vertex != this.maxDegreeVertex) {
				vertex.setVertexToShareInstancesWith(this.maxDegreeVertex);
			}
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
	public boolean preVisit(PlanNode node) {
		// check if we have visited this node before. in non-tree graphs, this happens
		if (this.vertices.containsKey(node) || this.chainedTasks.containsKey(node)) {
			// return false to prevent further descend
			return false;
		}

		// the vertex to be created for the current node
		final AbstractJobVertex vertex;
		try {
			if (node instanceof SingleInputPlanNode) {
				vertex = createSingleInputVertex((SingleInputPlanNode) node);
			}
			else if (node instanceof DualInputPlanNode) {
				vertex = createDualInputVertex((DualInputPlanNode) node);
			}
			else if (node instanceof SourcePlanNode) {
				vertex = createDataSourceVertex((SourcePlanNode) node);
			}
			else if (node instanceof SinkPlanNode) {
				vertex = createDataSinkVertex((SinkPlanNode) node);
			}
			else if (node instanceof UnionPlanNode) {
				// skip the union for now
				vertex = null;
			}
			else {
				throw new CompilerException("Unrecognized node type: " + node.getClass().getName());
			}
		}
		catch (Exception e) {
			throw new CompilerException("Error translating node '" + node + "': " + e.getMessage(), e);
		}
		
		// check if a vertex was created, or if it was chained or skipped
		if (vertex != null) {
			// set degree of parallelism
			int pd = node.getDegreeOfParallelism();
			vertex.setNumberOfSubtasks(pd);
	
			// check whether this is the vertex with the highest degree of parallelism
			if (this.maxDegreeVertex == null || this.maxDegreeVertex.getNumberOfSubtasks() < pd) {
				this.maxDegreeVertex = vertex;
			}
	
			// set the number of tasks per instance
			if (node.getSubtasksPerInstance() >= 1) {
				vertex.setNumberOfSubtasksPerInstance(node.getSubtasksPerInstance());
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
	public void postVisit(PlanNode node) {
		try {
			//do nothing for union nodes, we connect them later when gathering the inputs for a task
			if (node instanceof UnionPlanNode) {
				return;
			}
			
			// get the inputs and skip data sources
			final Iterator<Channel> inConns = node.getInputs();
			if (!inConns.hasNext()) {
				// data source
				return;
			}
			
			final AbstractJobVertex targetVertex = this.vertices.get(node);
			
			// check whether this node has its own task, or is chained to another one
			if (targetVertex == null) {
				// node's task is chained in another task
				final Channel inConn = inConns.next();
				
				//sanity check
				if (inConns.hasNext()) {
					throw new IllegalStateException("Bug: Found a chained task with more than one input!");
				}

				final TaskInChain chainedTask = this.chainedTasks.get(node);
				AbstractJobVertex container = chainedTask.getContainingVertex();
				
				if (container == null) {
					final PlanNode sourceNode = inConn.getSource();
					container = this.vertices.get(sourceNode);
					if (container == null) {
						// predecessor is itself chained
						container = this.chainedTasks.get(sourceNode).getContainingVertex();
						if (container == null)
							throw new IllegalStateException("Bug: Chained task predecessor has not been assigned its containing vertex.");
					} else {
						// predecessor is a proper task job vertex and this is the first chained task. add a forward connection entry.
						new TaskConfig(container.getConfiguration()).addOutputShipStrategy(ShipStrategyType.FORWARD);
					}
					chainedTask.setContainingVertex(container);
				}
				this.chainedTasksInSequence.add(chainedTask);
				return;
			}
			
			// this task it not chained.
			final TaskConfig targetVertexConfig = new TaskConfig(targetVertex.getConfiguration());
			
			for (int inputIndex = 0; inConns.hasNext(); inputIndex++) {
				final Channel input = inConns.next();
				
				// check that the type serializer is consistent
				TypeSerializerFactory<?> typeSerFact = null;
				
				// expand the channel to all the union channels, in case there is a union operator at its source
				for (Channel inConn : getConnectionsOfInput(input)) {
					// sanity check the common serializer
					if (typeSerFact == null) {
						typeSerFact = inConn.getSerializer();
					} else if (typeSerFact != inConn.getSerializer()) {
						throw new CompilerException("Conflicting types in union operator.");
					}
					
					final PlanNode sourceNode = inConn.getSource();
					AbstractJobVertex sourceVertex = this.vertices.get(sourceNode);
					TaskConfig sourceVertexConfig;
	
					if (sourceVertex == null) {
						// this predecessor is chained to another task
						final TaskInChain chainedTask = this.chainedTasks.get(sourceNode);
						if (chainedTask.getContainingVertex() == null)
							throw new IllegalStateException("Bug: Chained task has not been assigned its containing vertex when connecting.");
						sourceVertex = chainedTask.getContainingVertex();
						sourceVertexConfig = chainedTask.getTaskConfig();
					} else {
						sourceVertexConfig = new TaskConfig(sourceVertex.getConfiguration());
					}

					connectJobVertices(inConn, inputIndex, sourceVertex, sourceVertexConfig, targetVertex, targetVertexConfig);
				}
			}
		} catch (Exception e) {
			throw new CompilerException(
				"An error occurred while translating the optimized plan to a nephele JobGraph: " + e.getMessage(), e);
		}
	}
	
	private List<Channel> getConnectionsOfInput(Channel connection) {
		final PlanNode input = connection.getSource();
		if (input instanceof UnionPlanNode) {
			List<Channel> inConnOfInput = new ArrayList<Channel>();
			//create new connections between union inputs and target with properties of 
			//connection between target and union
			for (Iterator<Channel> inConns = input.getInputs(); inConns.hasNext();) {
				inConnOfInput.add(inConns.next());
			}
			return inConnOfInput;
		}
		else {
			return Collections.singletonList(connection);
		}
	}
	
	// ------------------------------------------------------------------------
	// Methods for creating individual vertices
	// ------------------------------------------------------------------------
	
	private JobTaskVertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException {
		final String taskName = node.getPactContract().getName();
		final DriverStrategy ds = node.getDriverStrategy();
		
		// check, whether chaining is possible
		boolean chaining = false;
		{
			Channel inConn = node.getInput();
			PlanNode pred = inConn.getSource();
			chaining = ds.getPushChainDriverClass() != null &&
					!(pred instanceof UnionPlanNode) &&
					inConn.getShipStrategy() == ShipStrategyType.FORWARD && 
					pred.getOutgoingChannels().size() == 1 &&
					node.getDegreeOfParallelism() == pred.getDegreeOfParallelism() && 
					node.getSubtasksPerInstance() == pred.getSubtasksPerInstance();
		}
		
		final JobTaskVertex vertex;
		final TaskConfig config;
		
		if (chaining) {
			vertex = null;
			config = new TaskConfig(new Configuration());
			this.chainedTasks.put(node, new TaskInChain(ChainedMapDriver.class, config, taskName));
		} else {
			// create task vertex
			vertex = new JobTaskVertex(taskName, this.jobGraph);
			vertex.setTaskClass(RegularPactTask.class);
			config = new TaskConfig(vertex.getConfiguration());
			config.setDriver(ds.getDriverClass());
		}
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		// set the driver strategy
		config.setDriverStrategy(ds);
		if (node.getComparator() != null) {
			config.setDriverComparator(node.getComparator(), 0);
		}
		
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobTaskVertex createDualInputVertex(DualInputPlanNode node) throws CompilerException {
		final String taskName = node.getPactContract().getName();
		final DriverStrategy ds = node.getDriverStrategy();
		final JobTaskVertex vertex = new JobTaskVertex(taskName, this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		vertex.setTaskClass(RegularPactTask.class);
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		// set the driver strategy
		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		if (node.getComparator1() != null) {
			config.setDriverComparator(node.getComparator1(), 0);
		}
		if (node.getComparator2() != null) {
			config.setDriverComparator(node.getComparator2(), 1);
		}
		if (node.getPairComparator() != null) {
			config.setDriverPairComparator(node.getPairComparator());
		}
		
		// assign memory, file-handles, etc.
		assignDriverResources(node, config);
		return vertex;
	}

	private JobInputVertex createDataSourceVertex(SourcePlanNode node) throws CompilerException {
		final JobInputVertex vertex = new JobInputVertex(node.getPactContract().getName(), this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		
		// set task class
		@SuppressWarnings("unchecked")
		final Class<AbstractInputTask<?>> clazz = (Class<AbstractInputTask<?>>) (Class<?>) DataSourceTask.class;
		vertex.setInputClass(clazz);

		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		return vertex;
	}

	private AbstractJobOutputVertex createDataSinkVertex(SinkPlanNode node) throws CompilerException {
		final JobOutputVertex vertex = new JobOutputVertex(node.getPactContract().getName(), this.jobGraph);
		final TaskConfig config = new TaskConfig(vertex.getConfiguration());
		
		vertex.setOutputClass(DataSinkTask.class);
		vertex.getConfiguration().setInteger(DataSinkTask.DEGREE_OF_PARALLELISM_KEY, node.getDegreeOfParallelism());
		
		// set user code
		config.setStubClass(node.getPactContract().getUserCodeClass());
		config.setStubParameters(node.getPactContract().getParameters());
		
		return vertex;
	}
	
	private void assignDriverResources(PlanNode node, TaskConfig config) {
		final long mem = node.getMemoryPerSubTask();
		if (mem > 0) {
			config.setMemoryDriver(mem);
		}
		config.setFilehandlesDriver(this.defaultMaxFan);
		config.setSpillingThresholdDriver(this.defaultSortSpillingThreshold);
	}

	// ------------------------------------------------------------------------
	// Connecting Vertices
	// ------------------------------------------------------------------------
	
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



	private void connectJobVertices(Channel connection, int inputNumber,
			final AbstractJobVertex sourceVertex, final TaskConfig sourceConfig,
			final AbstractJobVertex targetVertex, final TaskConfig targetConfig)
	throws JobGraphDefinitionException, CompilerException
	{
		final ChannelType channelType;
		final DistributionPattern distributionPattern;

		switch (connection.getShipStrategy()) {
			case FORWARD:
			case PARTITION_LOCAL_HASH:
			case PARTITION_LOCAL_RANGE:
				distributionPattern = DistributionPattern.POINTWISE;
				channelType = ChannelType.INMEMORY;
				break;
			case BROADCAST:
			case PARTITION_HASH:
			case PARTITION_RANGE:
				distributionPattern = DistributionPattern.BIPARTITE;
				channelType = ChannelType.NETWORK;
				break;
			default:
				throw new RuntimeException("Unknown runtime ship strategy: " + connection.getShipStrategy());
		}
		
		sourceVertex.connectTo(targetVertex, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
		sourceConfig.addInputToGroup(inputNumber);

		// configure the source task's ship strategy strategies in task config
		final int outputIndex = sourceConfig.getNumOutputs();
		sourceConfig.addOutputShipStrategy(connection.getShipStrategy());
		
		if (outputIndex == 0) {
			sourceConfig.setOutputSerializert(connection.getSerializer());
		}
		if (connection.getShipStrategyComparator() != null) {
			sourceConfig.setOutputComparator(connection.getShipStrategyComparator(), outputIndex);
		}
		
		// TODO: Re-enable range partitioner distribution
//		if (targetContract instanceof GenericDataSink) {
//			final DataDistribution distri = ((GenericDataSink) targetContract).getDataDistribution();
//			if (distri != null) {
//				configForOutputShipStrategy.setOutputDataDistribution(distri);
//			}
//		}
		
		// configure the receiving task's input serializer, comparator, ship strategy and temp mode
	}

	// ------------------------------------------------------------------------
	// Chained Tasks
	// ------------------------------------------------------------------------
	
	/**
	 * Utility class that describes a task in a sequence of chained tasks. Chained tasks are tasks that run
	 * together in one thread.
	 */
	private static final class TaskInChain
	{
		private final Class<? extends ChainedDriver<?, ?>> chainedTask;
		
		private final TaskConfig taskConfig;
		
		private final String taskName;
		
		private AbstractJobVertex containingVertex;

		@SuppressWarnings("unchecked")
		TaskInChain(@SuppressWarnings("rawtypes") Class<? extends ChainedDriver> chainedTask, TaskConfig taskConfig, String taskName) {
			this.chainedTask = (Class<? extends ChainedDriver<?, ?>>) chainedTask;
			this.taskConfig = taskConfig;
			this.taskName = taskName;
		}
		
		public Class<? extends ChainedDriver<?, ?>> getChainedTask() {
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
