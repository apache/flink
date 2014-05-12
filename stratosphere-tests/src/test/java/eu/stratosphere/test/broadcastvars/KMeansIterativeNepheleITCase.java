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
package eu.stratosphere.test.broadcastvars;

import org.apache.log4j.Level;

import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.CollectorMapDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.NoOpDriver;
import eu.stratosphere.pact.runtime.task.GroupReduceDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedCollectorMapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.test.iterative.nephele.JobGraphUtils;
import eu.stratosphere.test.recordJobs.kmeans.KMeans.PointBuilder;
import eu.stratosphere.test.recordJobs.kmeans.KMeans.RecomputeClusterCenter;
import eu.stratosphere.test.recordJobs.kmeans.KMeans.SelectNearestCenter;
import eu.stratosphere.test.recordJobs.kmeans.KMeans.PointOutFormat;
import eu.stratosphere.test.testdata.KMeansData;
import eu.stratosphere.test.util.RecordAPITestBase;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.LogUtils;


public class KMeansIterativeNepheleITCase extends RecordAPITestBase {

	private static final int ITERATION_ID = 42;
	
	private static final int MEMORY_PER_CONSUMER = 2;
	
	protected String dataPath;
	protected String clusterPath;
	protected String resultPath;

	
	public KMeansIterativeNepheleITCase() {
		LogUtils.initializeDefaultConsoleLogger(Level.ERROR);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", KMeansData.DATAPOINTS);
		clusterPath = createTempFile("initial_centers.txt", KMeansData.INITIAL_CENTERS);
		resultPath = getTempDirPath("result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(KMeansData.CENTERS_AFTER_20_ITERATIONS_SINGLE_DIGIT, resultPath);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		return createJobGraph(dataPath, clusterPath, this.resultPath, 4, 20);
	}

	// -------------------------------------------------------------------------------------------------------------
	// Job vertex builder methods
	// -------------------------------------------------------------------------------------------------------------

	private static JobInputVertex createPointsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		@SuppressWarnings("unchecked")
		CsvInputFormat pointsInFormat = new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class);
		JobInputVertex pointsInput = JobGraphUtils.createInput(pointsInFormat, pointsPath, "[Points]", jobGraph, numSubTasks, numSubTasks);
		{
			TaskConfig taskConfig = new TaskConfig(pointsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);
			
			TaskConfig chainedMapper = new TaskConfig(new Configuration());
			chainedMapper.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			chainedMapper.setStubWrapper(new UserCodeObjectWrapper<PointBuilder>(new PointBuilder()));
			chainedMapper.addOutputShipStrategy(ShipStrategyType.FORWARD);
			chainedMapper.setOutputSerializer(serializer);
			
			taskConfig.addChainedTask(ChainedCollectorMapDriver.class, chainedMapper, "Build points");
		}

		return pointsInput;
	}

	private static JobInputVertex createCentersInput(JobGraph jobGraph, String centersPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		@SuppressWarnings("unchecked")
		CsvInputFormat modelsInFormat = new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class);
		JobInputVertex modelsInput = JobGraphUtils.createInput(modelsInFormat, centersPath, "[Models]", jobGraph, numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(modelsInput.getConfiguration());
			taskConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			taskConfig.setOutputSerializer(serializer);

			TaskConfig chainedMapper = new TaskConfig(new Configuration());
			chainedMapper.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			chainedMapper.setStubWrapper(new UserCodeObjectWrapper<PointBuilder>(new PointBuilder()));
			chainedMapper.addOutputShipStrategy(ShipStrategyType.FORWARD);
			chainedMapper.setOutputSerializer(serializer);
			
			taskConfig.addChainedTask(ChainedCollectorMapDriver.class, chainedMapper, "Build centers");
		}

		return modelsInput;
	}

	private static JobOutputVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks, numSubTasks);

		{
			TaskConfig taskConfig = new TaskConfig(output.getConfiguration());
			taskConfig.addInputToGroup(0);
			taskConfig.setInputSerializer(serializer, 0);

			PointOutFormat outFormat = new PointOutFormat();
			outFormat.setOutputFilePath(new Path(resultPath));
			
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<PointOutFormat>(outFormat));
		}

		return output;
	}
	
	private static JobTaskVertex createIterationHead(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> serializer) {
		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Iteration Head", jobGraph, numSubTasks, numSubTasks);

		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setIterationId(ITERATION_ID);
		
		// initial input / partial solution
		headConfig.addInputToGroup(0);
		headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
		headConfig.setInputSerializer(serializer, 0);
		
		// back channel / iterations
		headConfig.setBackChannelMemory(MEMORY_PER_CONSUMER * JobGraphUtils.MEGABYTE);
		
		// output into iteration. broadcasting the centers
		headConfig.setOutputSerializer(serializer);
		headConfig.addOutputShipStrategy(ShipStrategyType.BROADCAST);
		
		// final output
		TaskConfig headFinalOutConfig = new TaskConfig(new Configuration());
		headFinalOutConfig.setOutputSerializer(serializer);
		headFinalOutConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		headConfig.setIterationHeadFinalOutputConfig(headFinalOutConfig);
		
		// the sync
		headConfig.setIterationHeadIndexOfSyncOutput(2);
		
		// the driver 
		headConfig.setDriver(NoOpDriver.class);
		headConfig.setDriverStrategy(DriverStrategy.UNARY_NO_OP);
		
		return head;
	}
	
	private static JobTaskVertex createMapper(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> inputSerializer,
			TypeSerializerFactory<?> broadcastVarSerializer, TypeSerializerFactory<?> outputSerializer,
			TypeComparatorFactory<?> outputComparator)
	{
		JobTaskVertex mapper = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Map (Select nearest center)", jobGraph, numSubTasks, numSubTasks);
		
		TaskConfig intermediateConfig = new TaskConfig(mapper.getConfiguration());
		intermediateConfig.setIterationId(ITERATION_ID);
		
		intermediateConfig.setDriver(CollectorMapDriver.class);
		intermediateConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
		intermediateConfig.addInputToGroup(0);
		intermediateConfig.setInputSerializer(inputSerializer, 0);
		
		intermediateConfig.setOutputSerializer(outputSerializer);
		intermediateConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		intermediateConfig.setOutputComparator(outputComparator, 0);

		intermediateConfig.setBroadcastInputName("centers", 0);
		intermediateConfig.addBroadcastInputToGroup(0);
		intermediateConfig.setBroadcastInputSerializer(broadcastVarSerializer, 0);
		
		// the udf
		intermediateConfig.setStubWrapper(new UserCodeObjectWrapper<SelectNearestCenter>(new SelectNearestCenter()));
		
		return mapper;
	}
	
	private static JobTaskVertex createReducer(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> inputSerializer,
			TypeComparatorFactory<?> inputComparator, TypeSerializerFactory<?> outputSerializer)
	{
		// ---------------- the tail (co group) --------------------
		
		JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "Reduce / Iteration Tail", jobGraph,
			numSubTasks, numSubTasks);
		
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		tailConfig.setIterationId(ITERATION_ID);
		tailConfig.setIsWorksetUpdate();
		
		// inputs and driver
		tailConfig.setDriver(GroupReduceDriver.class);
		tailConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
		tailConfig.addInputToGroup(0);
		tailConfig.setInputSerializer(inputSerializer, 0);		
		tailConfig.setDriverComparator(inputComparator, 0);

		tailConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
		tailConfig.setInputComparator(inputComparator, 0);
		tailConfig.setMemoryInput(0, MEMORY_PER_CONSUMER * JobGraphUtils.MEGABYTE);
		tailConfig.setFilehandlesInput(0, 128);
		tailConfig.setSpillingThresholdInput(0, 0.9f);
		
		// output
		tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		tailConfig.setOutputSerializer(outputSerializer);
		
		// the udf
		tailConfig.setStubWrapper(new UserCodeObjectWrapper<RecomputeClusterCenter>(new RecomputeClusterCenter()));
		
		return tail;
	}
	
	private static JobOutputVertex createSync(JobGraph jobGraph, int numIterations, int dop) {
		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, dop);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(numIterations);
		syncConfig.setIterationId(ITERATION_ID);
		return sync;
	}

	// -------------------------------------------------------------------------------------------------------------
	// Unified solution set and workset tail update
	// -------------------------------------------------------------------------------------------------------------

	private static JobGraph createJobGraph(String pointsPath, String centersPath, String resultPath, int numSubTasks, int numIterations) throws JobGraphDefinitionException {

		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> int0Comparator = new RecordComparatorFactory(new int[] { 0 }, new Class[] { IntValue.class });

		JobGraph jobGraph = new JobGraph("KMeans Iterative");

		// -- vertices ---------------------------------------------------------------------------------------------
		JobInputVertex points = createPointsInput(jobGraph, pointsPath, numSubTasks, serializer);
		JobInputVertex centers = createCentersInput(jobGraph, centersPath, numSubTasks, serializer);
		
		JobTaskVertex head = createIterationHead(jobGraph, numSubTasks, serializer);
		JobTaskVertex mapper = createMapper(jobGraph, numSubTasks, serializer, serializer, serializer, int0Comparator);
		
		JobTaskVertex reducer = createReducer(jobGraph, numSubTasks, serializer, int0Comparator, serializer);
		
		JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", numSubTasks, numSubTasks);
		
		JobOutputVertex sync = createSync(jobGraph, numIterations, numSubTasks);
		
		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(points, mapper, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(centers, head, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(head, mapper, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		new TaskConfig(mapper.getConfiguration()).setBroadcastGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);
		new TaskConfig(mapper.getConfiguration()).setInputCached(0, true);
		new TaskConfig(mapper.getConfiguration()).setInputMaterializationMemory(0, MEMORY_PER_CONSUMER * JobGraphUtils.MEGABYTE);

		JobGraphUtils.connect(mapper, reducer, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		new TaskConfig(reducer.getConfiguration()).setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);
		
		JobGraphUtils.connect(reducer, fakeTailOutput, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(head, output, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		
		

		// -- instance sharing -------------------------------------------------------------------------------------
		points.setVertexToShareInstancesWith(output);
		centers.setVertexToShareInstancesWith(output);
		head.setVertexToShareInstancesWith(output);
		mapper.setVertexToShareInstancesWith(output);
		reducer.setVertexToShareInstancesWith(output);
		fakeTailOutput.setVertexToShareInstancesWith(output);
		sync.setVertexToShareInstancesWith(output);

		return jobGraph;
	}
}
