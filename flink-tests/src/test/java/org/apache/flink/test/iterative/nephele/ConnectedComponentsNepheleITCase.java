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

package org.apache.flink.test.iterative.nephele;

import java.io.BufferedReader;
import java.util.Collection;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparatorFactory;
import org.apache.flink.api.common.typeutils.record.RecordPairComparatorFactory;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.record.operators.ReduceOperator.WrappingReduceFunction;
import org.apache.flink.api.java.record.operators.ReduceOperator.WrappingClassReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import org.apache.flink.runtime.iterative.task.IterationHeadPactTask;
import org.apache.flink.runtime.iterative.task.IterationIntermediatePactTask;
import org.apache.flink.runtime.iterative.task.IterationTailPactTask;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OutputFormatVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.BuildSecondCachedJoinDriver;
import org.apache.flink.runtime.operators.CollectorMapDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceDriver;
import org.apache.flink.runtime.operators.JoinWithSolutionSetSecondDriver;
import org.apache.flink.runtime.operators.chaining.ChainedCollectorMapDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.MinimumComponentIDReduce;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import org.apache.flink.test.recordJobs.graph.WorksetConnectedComponents.UpdateComponentIdMatch;
import org.apache.flink.test.testdata.ConnectedComponentsData;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the various variants of iteration state updates for workset iterations:
 * - unified solution set and workset tail update
 * - separate solution set and workset tail updates
 * - intermediate workset update and solution set tail
 * - intermediate solution set update and workset tail
 */
@SuppressWarnings("deprecation")
@RunWith(Parameterized.class)
public class ConnectedComponentsNepheleITCase extends RecordAPITestBase {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 1000;

	private static final int NUM_EDGES = 10000;

	private static final int ITERATION_ID = 1;

	private static final long MEM_PER_CONSUMER = 3;

	private static final int parallelism = 4;

	private static final double MEM_FRAC_PER_CONSUMER = (double)MEM_PER_CONSUMER/TASK_MANAGER_MEMORY_SIZE*parallelism;

	protected String verticesPath;

	protected String edgesPath;

	protected String resultPath;

	public ConnectedComponentsNepheleITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(parallelism);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("testcase", 1);

		Configuration config2 = new Configuration();
		config2.setInteger("testcase", 2);

		Configuration config3 = new Configuration();
		config3.setInteger("testcase", 3);

		Configuration config4 = new Configuration();
		config4.setInteger("testcase", 4);

		return toParameterList(config1, config2, config3, config4);
	}

	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		int maxIterations = 100;

		int type = config.getInteger("testcase", 0);
		switch (type) {
		case 1:
			return createJobGraphUnifiedTails(verticesPath, edgesPath, resultPath, parallelism, maxIterations);
		case 2:
			return createJobGraphSeparateTails(verticesPath, edgesPath, resultPath, parallelism, maxIterations);
		case 3:
			return createJobGraphIntermediateWorksetUpdateAndSolutionSetTail(verticesPath, edgesPath, resultPath, parallelism,
				maxIterations);
		case 4:
			return createJobGraphSolutionSetUpdateAndWorksetTail(verticesPath, edgesPath, resultPath, parallelism,
				maxIterations);
		default:
			throw new RuntimeException("Broken test configuration");
		}
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}

	public static final class IdDuplicator extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			record.setField(1, record.getField(0, LongValue.class));
			out.collect(record);
		}

	}

	// -----------------------------------------------------------------------------------------------------------------
	// Invariant vertices across all variants
	// -----------------------------------------------------------------------------------------------------------------

	private static InputFormatVertex createVerticesInput(JobGraph jobGraph, String verticesPath, int numSubTasks,
			TypeSerializerFactory<?> serializer, TypeComparatorFactory<?> comparator)
	{
		@SuppressWarnings("unchecked")
		CsvInputFormat verticesInFormat = new CsvInputFormat(' ', LongValue.class);
		InputFormatVertex verticesInput = JobGraphUtils.createInput(verticesInFormat, verticesPath, "VerticesInput",
			jobGraph, numSubTasks);
		TaskConfig verticesInputConfig = new TaskConfig(verticesInput.getConfiguration());
		{
			verticesInputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			verticesInputConfig.setOutputSerializer(serializer);

			// chained mapper that duplicates the id
			TaskConfig chainedMapperConfig = new TaskConfig(new Configuration());
			chainedMapperConfig.setStubWrapper(new UserCodeClassWrapper<IdDuplicator>(IdDuplicator.class));
			chainedMapperConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			chainedMapperConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			chainedMapperConfig.setInputSerializer(serializer, 0);

			chainedMapperConfig.setOutputSerializer(serializer);
			chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			chainedMapperConfig.setOutputComparator(comparator, 0);
			chainedMapperConfig.setOutputComparator(comparator, 1);

			verticesInputConfig.addChainedTask(ChainedCollectorMapDriver.class, chainedMapperConfig, "ID Duplicator");
		}

		return verticesInput;
	}

	private static InputFormatVertex createEdgesInput(JobGraph jobGraph, String edgesPath, int numSubTasks,
			TypeSerializerFactory<?> serializer, TypeComparatorFactory<?> comparator)
	{
		// edges
		@SuppressWarnings("unchecked")
		CsvInputFormat edgesInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class);
		InputFormatVertex edgesInput = JobGraphUtils.createInput(edgesInFormat, edgesPath, "EdgesInput", jobGraph,
			numSubTasks);
		TaskConfig edgesInputConfig = new TaskConfig(edgesInput.getConfiguration());
		{
			edgesInputConfig.setOutputSerializer(serializer);
			edgesInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			edgesInputConfig.setOutputComparator(comparator, 0);
		}

		return edgesInput;
	}

	private static JobVertex createIterationHead(JobGraph jobGraph, int numSubTasks,
			TypeSerializerFactory<?> serializer,
			TypeComparatorFactory<?> comparator,
			TypePairComparatorFactory<?, ?> pairComparator) {

		JobVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Join With Edges (Iteration Head)", jobGraph, numSubTasks);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		{
			headConfig.setIterationId(ITERATION_ID);

			// initial input / workset
			headConfig.addInputToGroup(0);
			headConfig.setInputSerializer(serializer, 0);
			headConfig.setInputComparator(comparator, 0);
			headConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);

			// regular plan input (second input to the join)
			headConfig.addInputToGroup(1);
			headConfig.setInputSerializer(serializer, 1);
			headConfig.setInputComparator(comparator, 1);
			headConfig.setInputLocalStrategy(1, LocalStrategy.NONE);
			headConfig.setInputCached(1, true);
			headConfig.setRelativeInputMaterializationMemory(1, MEM_FRAC_PER_CONSUMER);

			// initial solution set input
			headConfig.addInputToGroup(2);
			headConfig.setInputSerializer(serializer, 2);
			headConfig.setInputComparator(comparator, 2);
			headConfig.setInputLocalStrategy(2, LocalStrategy.NONE);
			headConfig.setIterationHeadSolutionSetInputIndex(2);

			headConfig.setSolutionSetSerializer(serializer);
			headConfig.setSolutionSetComparator(comparator);

			// back channel / iterations
			headConfig.setIsWorksetIteration();
			headConfig.setRelativeBackChannelMemory(MEM_FRAC_PER_CONSUMER);
			headConfig.setRelativeSolutionSetMemory(MEM_FRAC_PER_CONSUMER );

			// output into iteration
			headConfig.setOutputSerializer(serializer);
			headConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			headConfig.setOutputComparator(comparator, 0);

			// final output
			TaskConfig headFinalOutConfig = new TaskConfig(new Configuration());
			headFinalOutConfig.setOutputSerializer(serializer);
			headFinalOutConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			headConfig.setIterationHeadFinalOutputConfig(headFinalOutConfig);

			// the sync
			headConfig.setIterationHeadIndexOfSyncOutput(2);

			// the driver
			headConfig.setDriver(BuildSecondCachedJoinDriver.class);
			headConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			headConfig.setStubWrapper(
				new UserCodeClassWrapper<NeighborWithComponentIDJoin>(NeighborWithComponentIDJoin.class));
			headConfig.setDriverComparator(comparator, 0);
			headConfig.setDriverComparator(comparator, 1);
			headConfig.setDriverPairComparator(pairComparator);
			headConfig.setRelativeMemoryDriver(MEM_FRAC_PER_CONSUMER);

			headConfig.addIterationAggregator(
				WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, new LongSumAggregator());
		}

		return head;
	}

	private static JobVertex createIterationIntermediate(JobGraph jobGraph, int numSubTasks,
			TypeSerializerFactory<?> serializer, TypeComparatorFactory<?> comparator)
	{
		// --------------- the intermediate (reduce to min id) ---------------
		JobVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Find Min Component-ID", jobGraph, numSubTasks);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
		{
			intermediateConfig.setIterationId(ITERATION_ID);

			intermediateConfig.addInputToGroup(0);
			intermediateConfig.setInputSerializer(serializer, 0);
			intermediateConfig.setInputComparator(comparator, 0);
			intermediateConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
			intermediateConfig.setRelativeMemoryInput(0, MEM_FRAC_PER_CONSUMER);
			intermediateConfig.setFilehandlesInput(0, 64);
			intermediateConfig.setSpillingThresholdInput(0, 0.85f);

			intermediateConfig.setOutputSerializer(serializer);
			intermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);

			intermediateConfig.setDriver(GroupReduceDriver.class);
			intermediateConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
			intermediateConfig.setDriverComparator(comparator, 0);
			intermediateConfig.setStubWrapper(
				new UserCodeObjectWrapper<WrappingReduceFunction>(new WrappingClassReduceFunction(MinimumComponentIDReduce.class)));
		}

		return intermediate;
	}

	private static OutputFormatVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks,
			TypeSerializerFactory<?> serializer) {
		OutputFormatVertex output = JobGraphUtils.createFileOutput(jobGraph, "Final Output", numSubTasks);
		TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
		{

			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(serializer, 0);

			outputConfig.setStubWrapper(new UserCodeClassWrapper<CsvOutputFormat>(CsvOutputFormat.class));
			outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, resultPath);

			Configuration outputUserConfig = outputConfig.getStubParameters();
			outputUserConfig.setString(CsvOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			outputUserConfig.setString(CsvOutputFormat.FIELD_DELIMITER_PARAMETER, " ");
			outputUserConfig.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, LongValue.class);
			outputUserConfig.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 0);
			outputUserConfig.setClass(CsvOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, LongValue.class);
			outputUserConfig.setInteger(CsvOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 1);
			outputUserConfig.setInteger(CsvOutputFormat.NUM_FIELDS_PARAMETER, 2);
		}

		return output;
	}

	private static JobVertex createSync(JobGraph jobGraph, int numSubTasks, int maxIterations) {
		JobVertex sync = JobGraphUtils.createSync(jobGraph, numSubTasks);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(maxIterations);
		syncConfig.setIterationId(ITERATION_ID);
		syncConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME,
			new LongSumAggregator());
		syncConfig.setConvergenceCriterion(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME,
			new WorksetEmptyConvergenceCriterion());

		return sync;
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Unified solution set and workset tail update
	// -----------------------------------------------------------------------------------------------------------------

	public JobGraph createJobGraphUnifiedTails(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Unified Tails)");

		// -- invariant vertices -----------------------------------------------------------------------------------
		InputFormatVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		InputFormatVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);
		JobVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);

		JobVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		OutputFormatVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// --------------- the tail (solution set join) ---------------
		JobVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph, numSubTasks);
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		{
			tailConfig.setIterationId(ITERATION_ID);

			tailConfig.setIsWorksetIteration();
			tailConfig.setIsWorksetUpdate();

			tailConfig.setIsSolutionSetUpdate();
			tailConfig.setIsSolutionSetUpdateWithoutReprobe();

			// inputs and driver
			tailConfig.addInputToGroup(0);
			tailConfig.setInputSerializer(serializer, 0);

			// output
			tailConfig.setOutputSerializer(serializer);

			// the driver
			tailConfig.setDriver(JoinWithSolutionSetSecondDriver.class);
			tailConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			tailConfig.setDriverComparator(comparator, 0);
			tailConfig.setDriverPairComparator(pairComparator);
			
			tailConfig.setStubWrapper(new UserCodeClassWrapper<UpdateComponentIdMatch>(UpdateComponentIdMatch.class));
		}

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(edges, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);

		JobGraphUtils.connect(head, intermediate, DistributionPattern.ALL_TO_ALL);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, tail, DistributionPattern.POINTWISE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, DistributionPattern.POINTWISE);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		vertices.setSlotSharingGroup(sharingGroup);
		edges.setSlotSharingGroup(sharingGroup);
		head.setSlotSharingGroup(sharingGroup);
		intermediate.setSlotSharingGroup(sharingGroup);
		tail.setSlotSharingGroup(sharingGroup);
		output.setSlotSharingGroup(sharingGroup);
		sync.setSlotSharingGroup(sharingGroup);
		
		intermediate.setStrictlyCoLocatedWith(head);
		tail.setStrictlyCoLocatedWith(head);

		return jobGraph;
	}

	public JobGraph createJobGraphSeparateTails(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Unified Tails)");

		// input
		InputFormatVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		InputFormatVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setWaitForSolutionSetUpdate();

		// intermediate
		JobVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		OutputFormatVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ss join) ----------------------
		JobVertex ssJoinIntermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Solution Set Join", jobGraph, numSubTasks);
		TaskConfig ssJoinIntermediateConfig = new TaskConfig(ssJoinIntermediate.getConfiguration());
		{
			ssJoinIntermediateConfig.setIterationId(ITERATION_ID);

			// inputs
			ssJoinIntermediateConfig.addInputToGroup(0);
			ssJoinIntermediateConfig.setInputSerializer(serializer, 0);

			// output
			ssJoinIntermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			ssJoinIntermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			ssJoinIntermediateConfig.setOutputComparator(comparator, 0);
			ssJoinIntermediateConfig.setOutputComparator(comparator, 1);

			ssJoinIntermediateConfig.setOutputSerializer(serializer);

			// driver
			ssJoinIntermediateConfig.setDriver(JoinWithSolutionSetSecondDriver.class);
			ssJoinIntermediateConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			ssJoinIntermediateConfig.setDriverComparator(comparator, 0);
			ssJoinIntermediateConfig.setDriverPairComparator(pairComparator);
			
			ssJoinIntermediateConfig.setStubWrapper(
				new UserCodeClassWrapper<UpdateComponentIdMatch>(UpdateComponentIdMatch.class));
		}

		// -------------------------- ss tail --------------------------------
		JobVertex ssTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationSolutionSetTail",
			jobGraph, numSubTasks);
		TaskConfig ssTailConfig = new TaskConfig(ssTail.getConfiguration());
		{
			ssTailConfig.setIterationId(ITERATION_ID);
			ssTailConfig.setIsSolutionSetUpdate();
			ssTailConfig.setIsWorksetIteration();

			// inputs and driver
			ssTailConfig.addInputToGroup(0);
			ssTailConfig.setInputSerializer(serializer, 0);
			ssTailConfig.setInputAsynchronouslyMaterialized(0, true);
			ssTailConfig.setRelativeInputMaterializationMemory(0, MEM_FRAC_PER_CONSUMER);

			// output
			ssTailConfig.setOutputSerializer(serializer);

			// the driver
			ssTailConfig.setDriver(CollectorMapDriver.class);
			ssTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			ssTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// -------------------------- ws tail --------------------------------
		JobVertex wsTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationWorksetTail",
			jobGraph, numSubTasks);
		TaskConfig wsTailConfig = new TaskConfig(wsTail.getConfiguration());
		{
			wsTailConfig.setIterationId(ITERATION_ID);
			wsTailConfig.setIsWorksetIteration();
			wsTailConfig.setIsWorksetUpdate();

			// inputs and driver
			wsTailConfig.addInputToGroup(0);
			wsTailConfig.setInputSerializer(serializer, 0);

			// output
			wsTailConfig.setOutputSerializer(serializer);

			// the driver
			wsTailConfig.setDriver(CollectorMapDriver.class);
			wsTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			wsTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// --------------- the wiring ---------------------

		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(edges, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);

		JobGraphUtils.connect(head, intermediate, DistributionPattern.ALL_TO_ALL);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, ssJoinIntermediate, DistributionPattern.POINTWISE);
		ssJoinIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, ssTail, DistributionPattern.POINTWISE);
		ssTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, wsTail, DistributionPattern.POINTWISE);
		wsTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, DistributionPattern.POINTWISE);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		vertices.setSlotSharingGroup(sharingGroup);
		edges.setSlotSharingGroup(sharingGroup);
		head.setSlotSharingGroup(sharingGroup);
		intermediate.setSlotSharingGroup(sharingGroup);
		ssJoinIntermediate.setSlotSharingGroup(sharingGroup);
		wsTail.setSlotSharingGroup(sharingGroup);
		ssTail.setSlotSharingGroup(sharingGroup);
		output.setSlotSharingGroup(sharingGroup);
		sync.setSlotSharingGroup(sharingGroup);
		
		intermediate.setStrictlyCoLocatedWith(head);
		ssJoinIntermediate.setStrictlyCoLocatedWith(head);
		wsTail.setStrictlyCoLocatedWith(head);
		ssTail.setStrictlyCoLocatedWith(head);

		return jobGraph;
	}

	public JobGraph createJobGraphIntermediateWorksetUpdateAndSolutionSetTail(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Intermediate Workset Update, Solution Set Tail)");

		// input
		InputFormatVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		InputFormatVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setWaitForSolutionSetUpdate();

		// intermediate
		JobVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		JobVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ws update) ----------------------
		JobVertex wsUpdateIntermediate =
			JobGraphUtils.createTask(IterationIntermediatePactTask.class, "WorksetUpdate", jobGraph, numSubTasks);
		TaskConfig wsUpdateConfig = new TaskConfig(wsUpdateIntermediate.getConfiguration());
		{
			wsUpdateConfig.setIterationId(ITERATION_ID);
			wsUpdateConfig.setIsWorksetIteration();
			wsUpdateConfig.setIsWorksetUpdate();

			// inputs
			wsUpdateConfig.addInputToGroup(0);
			wsUpdateConfig.setInputSerializer(serializer, 0);

			// output
			wsUpdateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			wsUpdateConfig.setOutputComparator(comparator, 0);

			wsUpdateConfig.setOutputSerializer(serializer);

			// driver
			wsUpdateConfig.setDriver(JoinWithSolutionSetSecondDriver.class);
			wsUpdateConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			wsUpdateConfig.setDriverComparator(comparator, 0);
			wsUpdateConfig.setDriverPairComparator(pairComparator);
			
			wsUpdateConfig.setStubWrapper(new UserCodeClassWrapper<UpdateComponentIdMatch>(
				UpdateComponentIdMatch.class));
		}

		// -------------------------- ss tail --------------------------------
		JobVertex ssTail =
			JobGraphUtils.createTask(IterationTailPactTask.class, "IterationSolutionSetTail", jobGraph, numSubTasks);
		TaskConfig ssTailConfig = new TaskConfig(ssTail.getConfiguration());
		{
			ssTailConfig.setIterationId(ITERATION_ID);
			ssTailConfig.setIsSolutionSetUpdate();
			ssTailConfig.setIsWorksetIteration();

			// inputs and driver
			ssTailConfig.addInputToGroup(0);
			ssTailConfig.setInputSerializer(serializer, 0);

			// output
			ssTailConfig.setOutputSerializer(serializer);

			// the driver
			ssTailConfig.setDriver(CollectorMapDriver.class);
			ssTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			ssTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// edges

		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(edges, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);

		JobGraphUtils.connect(head, intermediate, DistributionPattern.ALL_TO_ALL);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, wsUpdateIntermediate,
				DistributionPattern.POINTWISE);
		wsUpdateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(wsUpdateIntermediate, ssTail, DistributionPattern.POINTWISE);
		ssTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, DistributionPattern.POINTWISE);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		vertices.setSlotSharingGroup(sharingGroup);
		edges.setSlotSharingGroup(sharingGroup);
		head.setSlotSharingGroup(sharingGroup);
		intermediate.setSlotSharingGroup(sharingGroup);
		wsUpdateIntermediate.setSlotSharingGroup(sharingGroup);
		ssTail.setSlotSharingGroup(sharingGroup);
		output.setSlotSharingGroup(sharingGroup);
		sync.setSlotSharingGroup(sharingGroup);

		intermediate.setStrictlyCoLocatedWith(head);
		wsUpdateIntermediate.setStrictlyCoLocatedWith(head);
		ssTail.setStrictlyCoLocatedWith(head);

		return jobGraph;
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Intermediate solution set update and workset tail
	// -----------------------------------------------------------------------------------------------------------------

	public JobGraph createJobGraphSolutionSetUpdateAndWorksetTail(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Intermediate Solution Set Update, Workset Tail)");

		// input
		InputFormatVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		InputFormatVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);

		// intermediate
		JobVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		JobVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ss update) ----------------------
		JobVertex ssJoinIntermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Solution Set Update", jobGraph, numSubTasks);
		TaskConfig ssJoinIntermediateConfig = new TaskConfig(ssJoinIntermediate.getConfiguration());
		{
			ssJoinIntermediateConfig.setIterationId(ITERATION_ID);
			ssJoinIntermediateConfig.setIsSolutionSetUpdate();
			ssJoinIntermediateConfig.setIsSolutionSetUpdateWithoutReprobe();

			// inputs
			ssJoinIntermediateConfig.addInputToGroup(0);
			ssJoinIntermediateConfig.setInputSerializer(serializer, 0);

			// output
			ssJoinIntermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			ssJoinIntermediateConfig.setOutputComparator(comparator, 0);

			ssJoinIntermediateConfig.setOutputSerializer(serializer);

			// driver
			ssJoinIntermediateConfig.setDriver(JoinWithSolutionSetSecondDriver.class);
			ssJoinIntermediateConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			ssJoinIntermediateConfig.setDriverComparator(comparator, 0);
			ssJoinIntermediateConfig.setDriverPairComparator(pairComparator);
			
			ssJoinIntermediateConfig.setStubWrapper(new UserCodeClassWrapper<UpdateComponentIdMatch>(UpdateComponentIdMatch.class));
		}

		// -------------------------- ws tail --------------------------------
		JobVertex wsTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationWorksetTail", jobGraph, numSubTasks);
		TaskConfig wsTailConfig = new TaskConfig(wsTail.getConfiguration());
		{
			wsTailConfig.setIterationId(ITERATION_ID);
			wsTailConfig.setIsWorksetIteration();
			wsTailConfig.setIsWorksetUpdate();

			// inputs and driver
			wsTailConfig.addInputToGroup(0);
			wsTailConfig.setInputSerializer(serializer, 0);

			// output
			wsTailConfig.setOutputSerializer(serializer);

			// the driver
			wsTailConfig.setDriver(CollectorMapDriver.class);
			wsTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			wsTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// --------------- the wiring ---------------------

		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(edges, head, DistributionPattern.ALL_TO_ALL);
		JobGraphUtils.connect(vertices, head, DistributionPattern.ALL_TO_ALL);

		JobGraphUtils.connect(head, intermediate, DistributionPattern.ALL_TO_ALL);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, ssJoinIntermediate, DistributionPattern.POINTWISE);
		ssJoinIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, wsTail, DistributionPattern.POINTWISE);
		wsTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, DistributionPattern.POINTWISE);

		
		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		vertices.setSlotSharingGroup(sharingGroup);
		edges.setSlotSharingGroup(sharingGroup);
		head.setSlotSharingGroup(sharingGroup);
		intermediate.setSlotSharingGroup(sharingGroup);
		ssJoinIntermediate.setSlotSharingGroup(sharingGroup);
		wsTail.setSlotSharingGroup(sharingGroup);
		output.setSlotSharingGroup(sharingGroup);
		sync.setSlotSharingGroup(sharingGroup);

		intermediate.setStrictlyCoLocatedWith(head);
		ssJoinIntermediate.setStrictlyCoLocatedWith(head);
		wsTail.setStrictlyCoLocatedWith(head);

		return jobGraph;
	}

	public static final class DummyMapper extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record rec, Collector<Record> out) {
			out.collect(rec);
		}
	}
}
