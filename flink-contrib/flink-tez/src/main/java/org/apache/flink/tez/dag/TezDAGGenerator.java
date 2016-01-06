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

package org.apache.flink.tez.dag;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.tez.runtime.TezTaskConfig;
import org.apache.flink.util.Visitor;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class TezDAGGenerator implements Visitor<PlanNode> {

	private static final Log LOG = LogFactory.getLog(TezDAGGenerator.class);
	
	private Map<PlanNode, FlinkVertex> vertices; // a map from optimizer nodes to Tez vertices
	private List<FlinkEdge> edges;
	private final int defaultMaxFan;
	private final TezConfiguration tezConf;

	private final float defaultSortSpillingThreshold;

	public TezDAGGenerator (TezConfiguration tezConf, Configuration config) {
		this.defaultMaxFan = config.getInteger(ConfigConstants.DEFAULT_SPILLING_MAX_FAN_KEY,
				ConfigConstants.DEFAULT_SPILLING_MAX_FAN);
		this.defaultSortSpillingThreshold = config.getFloat(ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD_KEY,
				ConfigConstants.DEFAULT_SORT_SPILLING_THRESHOLD);
		this.tezConf = tezConf;
	}

	public DAG createDAG (OptimizedPlan program) throws Exception {
		LOG.info ("Creating Tez DAG");
		this.vertices = new HashMap<PlanNode, FlinkVertex>();
		this.edges = new ArrayList<FlinkEdge>();
		program.accept(this);

		DAG dag = DAG.create(program.getJobName());
		for (FlinkVertex v : vertices.values()) {
			dag.addVertex(v.createVertex(new TezConfiguration(tezConf)));
		}
		for (FlinkEdge e: edges) {
			dag.addEdge(e.createEdge(new TezConfiguration(tezConf)));
		}

		/*
		 * Temporarily throw an error until TEZ-1190 has been fixed or a workaround has been created
		 */
		if (containsSelfJoins()) {
			throw new CompilerException("Dual-input operators with the same input (self-joins) are not yet supported");
		}

		this.vertices = null;
		this.edges = null;

		LOG.info ("Tez DAG created");
		return dag;
	}


	@Override
	public boolean preVisit(PlanNode node) {
		if (this.vertices.containsKey(node)) {
			// return false to prevent further descend
			return false;
		}

		if ((node instanceof BulkIterationPlanNode) || (node instanceof WorksetIterationPlanNode)) {
			throw new CompilerException("Iterations are not yet supported by the Tez execution environment");
		}

		if ( (node.getBroadcastInputs() != null) && (!node.getBroadcastInputs().isEmpty())) {
			throw new CompilerException("Broadcast inputs are not yet supported by the Tez execution environment");
		}

		FlinkVertex vertex = null;

		try {
			if (node instanceof SourcePlanNode) {
				vertex = createDataSourceVertex ((SourcePlanNode) node);
			}
			else if (node instanceof SinkPlanNode) {
				vertex = createDataSinkVertex ((SinkPlanNode) node);
			}
			else if ((node instanceof SingleInputPlanNode)) {
				vertex = createSingleInputVertex((SingleInputPlanNode) node);
			}
			else if (node instanceof DualInputPlanNode) {
				vertex = createDualInputVertex((DualInputPlanNode) node);
			}
			else if (node instanceof NAryUnionPlanNode) {
				vertex = createUnionVertex ((NAryUnionPlanNode) node);
			}
			else {
				throw new CompilerException("Unrecognized node type: " + node.getClass().getName());
			}

		}
		catch (Exception e) {
			throw new CompilerException("Error translating node '" + node + "': " + e.getMessage(), e);
		}

		if (vertex != null) {
			this.vertices.put(node, vertex);
		}
		return true;
	}

	@Override
	public void postVisit (PlanNode node) {
		try {
			if (node instanceof SourcePlanNode) {
				return;
			}
			final Iterator<Channel> inConns = node.getInputs().iterator();
			if (!inConns.hasNext()) {
				throw new CompilerException("Bug: Found a non-source task with no input.");
			}
			int inputIndex = 0;

			FlinkVertex targetVertex = this.vertices.get(node);
			TezTaskConfig targetVertexConfig = targetVertex.getConfig();


			while (inConns.hasNext()) {
				Channel input = inConns.next();
				inputIndex += translateChannel(input, inputIndex, targetVertex, targetVertexConfig, false);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			throw new CompilerException(
					"An error occurred while translating the optimized plan to a Tez DAG: " + e.getMessage(), e);
		}
	}

	private FlinkVertex createSingleInputVertex(SingleInputPlanNode node) throws CompilerException, IOException {

		final String taskName = node.getNodeName();
		final DriverStrategy ds = node.getDriverStrategy();
		final int dop = node.getParallelism();

		final TezTaskConfig config= new TezTaskConfig(new Configuration());

		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());

		for(int i=0;i<ds.getNumRequiredComparators();i++) {
			config.setDriverComparator(node.getComparator(i), i);
		}
		assignDriverResources(node, config);

		return new FlinkProcessorVertex(taskName, dop, config);
	}

	private FlinkVertex createDualInputVertex(DualInputPlanNode node) throws CompilerException, IOException {
		final String taskName = node.getNodeName();
		final DriverStrategy ds = node.getDriverStrategy();
		final int dop = node.getParallelism();

		final TezTaskConfig config= new TezTaskConfig(new Configuration());

		config.setDriver(ds.getDriverClass());
		config.setDriverStrategy(ds);
		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());

		if (node.getComparator1() != null) {
			config.setDriverComparator(node.getComparator1(), 0);
		}
		if (node.getComparator2() != null) {
			config.setDriverComparator(node.getComparator2(), 1);
		}
		if (node.getPairComparator() != null) {
			config.setDriverPairComparator(node.getPairComparator());
		}

		assignDriverResources(node, config);

		LOG.info("Creating processor vertex " + taskName + " with parallelism " + dop);

		return new FlinkProcessorVertex(taskName, dop, config);
	}

	private FlinkVertex createDataSinkVertex(SinkPlanNode node) throws CompilerException, IOException {
		final String taskName = node.getNodeName();
		final int dop = node.getParallelism();

		final TezTaskConfig config = new TezTaskConfig(new Configuration());

		// set user code
		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());

		LOG.info("Creating data sink vertex " + taskName + " with parallelism " + dop);
		
		return new FlinkDataSinkVertex(taskName, dop, config);
	}

	private FlinkVertex createDataSourceVertex(SourcePlanNode node) throws CompilerException, IOException {
		final String taskName = node.getNodeName();
		int dop = node.getParallelism();

		final TezTaskConfig config= new TezTaskConfig(new Configuration());

		config.setStubWrapper(node.getProgramOperator().getUserCodeWrapper());
		config.setStubParameters(node.getProgramOperator().getParameters());

		InputFormat format = node.getDataSourceNode().getOperator().getFormatWrapper().getUserCodeObject();

		config.setInputFormat(format);

		// Create as many data sources as input splits
		InputSplit[] splits = format.createInputSplits((dop > 0) ? dop : 1);
		dop = splits.length;

		LOG.info("Creating data source vertex " + taskName + " with parallelism " + dop);
		
		return new FlinkDataSourceVertex(taskName, dop, config);
	}

	private FlinkVertex createUnionVertex(NAryUnionPlanNode node) throws CompilerException, IOException {
		final String taskName = node.getNodeName();
		final int dop = node.getParallelism();
		final TezTaskConfig config= new TezTaskConfig(new Configuration());

		LOG.info("Creating union vertex " + taskName + " with parallelism " + dop);
		
		return new FlinkUnionVertex (taskName, dop, config);
	}


	private void assignDriverResources(PlanNode node, TaskConfig config) {
		final double relativeMem = node.getRelativeMemoryPerSubTask();
		if (relativeMem > 0) {
			config.setRelativeMemoryDriver(relativeMem);
			config.setFilehandlesDriver(this.defaultMaxFan);
			config.setSpillingThresholdDriver(this.defaultSortSpillingThreshold);
		}
	}

	private void assignLocalStrategyResources(Channel c, TaskConfig config, int inputNum) {
		if (c.getRelativeMemoryLocalStrategy() > 0) {
			config.setRelativeMemoryInput(inputNum, c.getRelativeMemoryLocalStrategy());
			config.setFilehandlesInput(inputNum, this.defaultMaxFan);
			config.setSpillingThresholdInput(inputNum, this.defaultSortSpillingThreshold);
		}
	}

	private int translateChannel(Channel input, int inputIndex, FlinkVertex targetVertex,
								TezTaskConfig targetVertexConfig, boolean isBroadcast) throws Exception
	{
		final PlanNode inputPlanNode = input.getSource();
		final Iterator<Channel> allInChannels;


		allInChannels = Collections.singletonList(input).iterator();


		// check that the type serializer is consistent
		TypeSerializerFactory<?> typeSerFact = null;

		while (allInChannels.hasNext()) {
			final Channel inConn = allInChannels.next();

			if (typeSerFact == null) {
				typeSerFact = inConn.getSerializer();
			} else if (!typeSerFact.equals(inConn.getSerializer())) {
				throw new CompilerException("Conflicting types in union operator.");
			}

			final PlanNode sourceNode = inConn.getSource();
			FlinkVertex sourceVertex = this.vertices.get(sourceNode);
			TezTaskConfig sourceVertexConfig = sourceVertex.getConfig(); //TODO ??? need to create a new TezConfig ???

			connectJobVertices(
					inConn, inputIndex, sourceVertex, sourceVertexConfig, targetVertex, targetVertexConfig, isBroadcast);
		}

		// the local strategy is added only once. in non-union case that is the actual edge,
		// in the union case, it is the edge between union and the target node
		addLocalInfoFromChannelToConfig(input, targetVertexConfig, inputIndex, isBroadcast);
		return 1;
	}

	private void connectJobVertices(Channel channel, int inputNumber,
							final FlinkVertex sourceVertex, final TezTaskConfig sourceConfig,
							final FlinkVertex targetVertex, final TezTaskConfig targetConfig, boolean isBroadcast)
			throws CompilerException {

		// -------------- configure the source task's ship strategy strategies in task config --------------
		final int outputIndex = sourceConfig.getNumOutputs();
		sourceConfig.addOutputShipStrategy(channel.getShipStrategy());
		if (outputIndex == 0) {
			sourceConfig.setOutputSerializer(channel.getSerializer());
		}
		if (channel.getShipStrategyComparator() != null) {
			sourceConfig.setOutputComparator(channel.getShipStrategyComparator(), outputIndex);
		}

		if (channel.getShipStrategy() == ShipStrategyType.PARTITION_RANGE) {

			final DataDistribution dataDistribution = channel.getDataDistribution();
			if(dataDistribution != null) {
				sourceConfig.setOutputDataDistribution(dataDistribution, outputIndex);
			} else {
				throw new RuntimeException("Range partitioning requires data distribution");
				// TODO: inject code and configuration for automatic histogram generation
			}
		}

		// ---------------- configure the receiver -------------------
		if (isBroadcast) {
			targetConfig.addBroadcastInputToGroup(inputNumber);
		} else {
			targetConfig.addInputToGroup(inputNumber);
		}

		//----------------- connect source and target with edge ------------------------------

		FlinkEdge edge;
		ShipStrategyType shipStrategy = channel.getShipStrategy();
		TypeSerializer<?> serializer = channel.getSerializer().getSerializer();
		if ((shipStrategy == ShipStrategyType.FORWARD) || (shipStrategy == ShipStrategyType.NONE)) {
			edge = new FlinkForwardEdge(sourceVertex, targetVertex, serializer);
			// For forward edges, create as many tasks in upstream operator as in source operator
			targetVertex.setParallelism(sourceVertex.getParallelism());
		}
		else if (shipStrategy == ShipStrategyType.BROADCAST) {
			edge = new FlinkBroadcastEdge(sourceVertex, targetVertex, serializer);
		}
		else if (shipStrategy == ShipStrategyType.PARTITION_HASH) {
			edge = new FlinkPartitionEdge(sourceVertex, targetVertex, serializer);
		}
		else {
			throw new CompilerException("Ship strategy between nodes " + sourceVertex.getVertex().getName() + " and " + targetVertex.getVertex().getName() + " currently not supported");
		}

		// Tez-specific bookkeeping
		// TODO: This probably will not work for vertices with multiple outputs
		sourceVertex.addNumberOfSubTasksInOutput(targetVertex.getParallelism(), outputIndex);
		targetVertex.addInput(sourceVertex, inputNumber);


		edges.add(edge);
	}

	private void addLocalInfoFromChannelToConfig(Channel channel, TaskConfig config, int inputNum, boolean isBroadcastChannel) {
		// serializer
		if (isBroadcastChannel) {
			config.setBroadcastInputSerializer(channel.getSerializer(), inputNum);

			if (channel.getLocalStrategy() != LocalStrategy.NONE || (channel.getTempMode() != null && channel.getTempMode() != TempMode.NONE)) {
				throw new CompilerException("Found local strategy or temp mode on a broadcast variable channel.");
			} else {
				return;
			}
		} else {
			config.setInputSerializer(channel.getSerializer(), inputNum);
		}

		// local strategy
		if (channel.getLocalStrategy() != LocalStrategy.NONE) {
			config.setInputLocalStrategy(inputNum, channel.getLocalStrategy());
			if (channel.getLocalStrategyComparator() != null) {
				config.setInputComparator(channel.getLocalStrategyComparator(), inputNum);
			}
		}

		assignLocalStrategyResources(channel, config, inputNum);

		// materialization / caching
		if (channel.getTempMode() != null) {
			final TempMode tm = channel.getTempMode();

			boolean needsMemory = false;
			if (tm.breaksPipeline()) {
				config.setInputAsynchronouslyMaterialized(inputNum, true);
				needsMemory = true;
			}
			if (tm.isCached()) {
				config.setInputCached(inputNum, true);
				needsMemory = true;
			}

			if (needsMemory) {
				// sanity check
				if (tm == null || tm == TempMode.NONE || channel.getRelativeTempMemory() <= 0) {
					throw new CompilerException("Bug in compiler: Inconsistent description of input materialization.");
				}
				config.setRelativeInputMaterializationMemory(inputNum, channel.getRelativeTempMemory());
			}
		}
	}

	private boolean containsSelfJoins () {
		for (FlinkVertex v : vertices.values()) {
			ArrayList<FlinkVertex> predecessors = new ArrayList<FlinkVertex>();
			for (FlinkEdge e : edges) {
				if (e.target == v) {
					if (predecessors.contains(e.source)) {
						return true;
					}
					predecessors.add(e.source);
				}
			}
		}
		return false;
	}

}
