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

package org.apache.flink.test.broadcastvars;

import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparatorFactory;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.operators.ReduceOperator.WrappingReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.iterative.task.IterationHeadPactTask;
import org.apache.flink.runtime.iterative.task.IterationIntermediatePactTask;
import org.apache.flink.runtime.iterative.task.IterationTailPactTask;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.CollectorMapDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceDriver;
import org.apache.flink.runtime.operators.NoOpDriver;
import org.apache.flink.runtime.operators.chaining.ChainedCollectorMapDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.test.iterative.nephele.JobGraphUtils;
import org.apache.flink.test.recordJobs.kmeans.KMeansBroadcast.PointBuilder;
import org.apache.flink.test.recordJobs.kmeans.KMeansBroadcast.PointOutFormat;
import org.apache.flink.test.recordJobs.kmeans.KMeansBroadcast.RecomputeClusterCenter;
import org.apache.flink.test.recordJobs.kmeans.KMeansBroadcast.SelectNearestCenter;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;

public class KMeansIterativeNepheleITCase extends RecordAPITestBase {

	private static final int ITERATION_ID = 42;
	
	private static final int MEMORY_PER_CONSUMER = 2;

	private static final int parallelism = 4;

	private static final double MEMORY_FRACTION_PER_CONSUMER = (double)MEMORY_PER_CONSUMER/TASK_MANAGER_MEMORY_SIZE*parallelism;

	protected String dataPath;
	protected String clusterPath;
	protected String resultPath;

	
	public KMeansIterativeNepheleITCase() {
		setTaskManagerNumSlots(parallelism);
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
		return createJobGraph(dataPath, clusterPath, this.resultPath, parallelism, 20);
	}

	// -------------------------------------------------------------------------------------------------------------
	// Job vertex builder methods
	// -------------------------------------------------------------------------------------------------------------

	private static InputFormatVertex createPointsInput(JobGraph jobGraph, String pointsPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		@SuppressWarnings("unchecked")
		CsvInputFormat pointsInFormat = new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class);
		InputFormatVertex pointsInput = JobGraphUtils.createInput(pointsInFormat, pointsPath, "[Points]", jobGraph, numSubTasks);
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

	private static InputFormatVertex createCentersInput(JobGraph jobGraph, String centersPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		@SuppressWarnings("unchecked")
		CsvInputFormat modelsInFormat = new CsvInputFormat('|', IntValue.class, DoubleValue.class, DoubleValue.class, DoubleValue.class);
		InputFormatVertex modelsInput = JobGraphUtils.createInput(modelsInFormat, centersPath, "[Models]", jobGraph, numSubTasks);

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

	private static OutputFormatVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks, TypeSerializerFactory<?> serializer) {
		
		OutputFormatVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks);

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
	
	private static AbstractJobVertex createIterationHead(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> serializer) {
		AbstractJobVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Iteration Head", jobGraph, numSubTasks);

		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setIterationId(ITERATION_ID);
		
		// initial input / partial solution
		headConfig.addInputToGroup(0);
		headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);
		headConfig.setInputSerializer(serializer, 0);
		
		// back channel / iterations
		headConfig.setRelativeBackChannelMemory(MEMORY_FRACTION_PER_CONSUMER);
		
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
	
	private static AbstractJobVertex createMapper(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> inputSerializer,
			TypeSerializerFactory<?> broadcastVarSerializer, TypeSerializerFactory<?> outputSerializer,
			TypeComparatorFactory<?> outputComparator)
	{
		AbstractJobVertex mapper = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Map (Select nearest center)", jobGraph, numSubTasks);
		
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
	
	private static AbstractJobVertex createReducer(JobGraph jobGraph, int numSubTasks, TypeSerializerFactory<?> inputSerializer,
			TypeComparatorFactory<?> inputComparator, TypeSerializerFactory<?> outputSerializer)
	{
		// ---------------- the tail (reduce) --------------------
		
		AbstractJobVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "Reduce / Iteration Tail", jobGraph,
			numSubTasks);
		
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
		tailConfig.setRelativeMemoryInput(0, MEMORY_FRACTION_PER_CONSUMER);
		tailConfig.setFilehandlesInput(0, 128);
		tailConfig.setSpillingThresholdInput(0, 0.9f);
		
		// output
		tailConfig.setOutputSerializer(outputSerializer);
		
		// the udf
		tailConfig.setStubWrapper(new UserCodeObjectWrapper<WrappingReduceFunction>(new WrappingReduceFunction(new RecomputeClusterCenter())));
		
		return tail;
	}
	
	private static AbstractJobVertex createSync(JobGraph jobGraph, int numIterations, int parallelism) {
		AbstractJobVertex sync = JobGraphUtils.createSync(jobGraph, parallelism);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(numIterations);
		syncConfig.setIterationId(ITERATION_ID);
		return sync;
	}

	// -------------------------------------------------------------------------------------------------------------
	// Unified solution set and workset tail update
	// -------------------------------------------------------------------------------------------------------------

	private static JobGraph createJobGraph(String pointsPath, String centersPath, String resultPath, int numSubTasks, int numIterations) {

		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> int0Comparator = new RecordComparatorFactory(new int[] { 0 }, new Class[] { IntValue.class });

		JobGraph jobGraph = new JobGraph("KMeans Iterative");

		// -- vertices ---------------------------------------------------------------------------------------------
		InputFormatVertex points = createPointsInput(jobGraph, pointsPath, numSubTasks, serializer);
		InputFormatVertex centers = createCentersInput(jobGraph, centersPath, numSubTasks, serializer);
		
		AbstractJobVertex head = createIterationHead(jobGraph, numSubTasks, serializer);
		AbstractJobVertex mapper = createMapper(jobGraph, numSubTasks, serializer, serializer, serializer, int0Comparator);
		
		AbstractJobVertex reducer = createReducer(jobGraph, numSubTasks, serializer, int0Comparator, serializer);
		
		AbstractJobVertex sync = createSync(jobGraph, numIterations, numSubTasks);
		
		OutputFormatVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(points, mapper, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(centers, head, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(head, mapper, DistributionPattern.ALL_TO_ALL);
		new TaskConfig(mapper.getConfiguration()).setBroadcastGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);
		new TaskConfig(mapper.getConfiguration()).setInputCached(0, true);
		new TaskConfig(mapper.getConfiguration()).setRelativeInputMaterializationMemory(0,
				MEMORY_FRACTION_PER_CONSUMER);

		JobGraphUtils.connect(mapper, reducer, DistributionPattern.ALL_TO_ALL);
		new TaskConfig(reducer.getConfiguration()).setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);
		
		JobGraphUtils.connect(head, output, DistributionPattern.POINTWISE);
		
		JobGraphUtils.connect(head, sync, DistributionPattern.ALL_TO_ALL);

		// -- instance sharing -------------------------------------------------------------------------------------
		
		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		
		points.setSlotSharingGroup(sharingGroup);
		centers.setSlotSharingGroup(sharingGroup);
		head.setSlotSharingGroup(sharingGroup);
		mapper.setSlotSharingGroup(sharingGroup);
		reducer.setSlotSharingGroup(sharingGroup);
		sync.setSlotSharingGroup(sharingGroup);
		output.setSlotSharingGroup(sharingGroup);
		
		mapper.setStrictlyCoLocatedWith(head);
		reducer.setStrictlyCoLocatedWith(head);

		return jobGraph;
	}
}
