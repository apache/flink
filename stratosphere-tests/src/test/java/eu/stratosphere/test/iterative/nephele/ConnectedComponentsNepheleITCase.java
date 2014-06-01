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

package eu.stratosphere.test.iterative.nephele;

import java.io.BufferedReader;
import java.util.Collection;

import eu.stratosphere.nephele.jobgraph.DistributionPattern;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.common.aggregators.LongSumAggregator;
import eu.stratosphere.api.common.operators.util.UserCodeClassWrapper;
import eu.stratosphere.api.common.typeutils.TypeComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypePairComparatorFactory;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.FileOutputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordPairComparatorFactory;
import eu.stratosphere.api.java.typeutils.runtime.record.RecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.BuildSecondCachedMatchDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.CollectorMapDriver;
import eu.stratosphere.pact.runtime.task.JoinWithSolutionSetSecondDriver;
import eu.stratosphere.pact.runtime.task.GroupReduceDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedCollectorMapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.test.recordJobs.graph.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.test.recordJobs.graph.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.test.recordJobs.graph.WorksetConnectedComponents.UpdateComponentIdMatch;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * Tests the various variants of iteration state updates for workset iterations:
 * - unified solution set and workset tail update
 * - separate solution set and workset tail updates
 * - intermediate workset update and solution set tail
 * - intermediate solution set update and workset tail
 */
@RunWith(Parameterized.class)
public class ConnectedComponentsNepheleITCase extends RecordAPITestBase {

	private static final long SEED = 0xBADC0FFEEBEEFL;

	private static final int NUM_VERTICES = 1000;

	private static final int NUM_EDGES = 10000;

	private static final int ITERATION_ID = 1;

	private static final long MEM_PER_CONSUMER = 3;

	private static final int DOP = 4;

	private static final double MEM_FRAC_PER_CONSUMER = (double)MEM_PER_CONSUMER/TASK_MANAGER_MEMORY_SIZE*DOP;

	protected String verticesPath;

	protected String edgesPath;

	protected String resultPath;

	public ConnectedComponentsNepheleITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(DOP);
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
			return createJobGraphUnifiedTails(verticesPath, edgesPath, resultPath, DOP, maxIterations);
		case 2:
			return createJobGraphSeparateTails(verticesPath, edgesPath, resultPath, DOP, maxIterations);
		case 3:
			return createJobGraphIntermediateWorksetUpdateAndSolutionSetTail(verticesPath, edgesPath, resultPath, DOP,
				maxIterations);
		case 4:
			return createJobGraphSolutionSetUpdateAndWorksetTail(verticesPath, edgesPath, resultPath, DOP,
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

	private static JobInputVertex createVerticesInput(JobGraph jobGraph, String verticesPath, int numSubTasks,
			TypeSerializerFactory<?> serializer,
			TypeComparatorFactory<?> comparator) {
		@SuppressWarnings("unchecked")
		CsvInputFormat verticesInFormat = new CsvInputFormat(' ', LongValue.class);
		JobInputVertex verticesInput = JobGraphUtils.createInput(verticesInFormat, verticesPath, "VerticesInput",
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

	private static JobInputVertex createEdgesInput(JobGraph jobGraph, String edgesPath, int numSubTasks,
			TypeSerializerFactory<?> serializer,
			TypeComparatorFactory<?> comparator) {
		// edges
		@SuppressWarnings("unchecked")
		CsvInputFormat edgesInFormat = new CsvInputFormat(' ', LongValue.class, LongValue.class);
		JobInputVertex edgesInput = JobGraphUtils.createInput(edgesInFormat, edgesPath, "EdgesInput", jobGraph,
			numSubTasks);
		TaskConfig edgesInputConfig = new TaskConfig(edgesInput.getConfiguration());
		{
			edgesInputConfig.setOutputSerializer(serializer);
			edgesInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			edgesInputConfig.setOutputComparator(comparator, 0);
		}

		return edgesInput;
	}

	private static JobTaskVertex createIterationHead(JobGraph jobGraph, int numSubTasks,
			TypeSerializerFactory<?> serializer,
			TypeComparatorFactory<?> comparator,
			TypePairComparatorFactory<?, ?> pairComparator) {

		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Join With Edges (Iteration Head)",
			jobGraph, numSubTasks);
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
			headConfig.setDriver(BuildSecondCachedMatchDriver.class);
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

	private static JobTaskVertex createIterationIntermediate(JobGraph jobGraph, int numSubTasks,
			TypeSerializerFactory<?> serializer,
			TypeComparatorFactory<?> comparator) {

		// --------------- the intermediate (reduce to min id) ---------------
		JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
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
				new UserCodeClassWrapper<MinimumComponentIDReduce>(MinimumComponentIDReduce.class));
		}

		return intermediate;
	}

	private static JobOutputVertex createOutput(JobGraph jobGraph, String resultPath, int numSubTasks,
			TypeSerializerFactory<?> serializer) {
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Final Output", numSubTasks);
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

	private static JobOutputVertex createFakeTail(JobGraph jobGraph, int numSubTasks) {
		JobOutputVertex fakeTailOutput =
			JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput", numSubTasks);
		return fakeTailOutput;
	}

	private static JobOutputVertex createSync(JobGraph jobGraph, int numSubTasks, int maxIterations) {
		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, numSubTasks);
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
			throws JobGraphDefinitionException
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Unified Tails)");

		// -- invariant vertices -----------------------------------------------------------------------------------
		JobInputVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		JobInputVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);
		JobTaskVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);

		JobTaskVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobOutputVertex fakeTail = createFakeTail(jobGraph, numSubTasks);
		JobOutputVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// --------------- the tail (solution set join) ---------------
		JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
			numSubTasks);
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
			tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			tailConfig.setOutputSerializer(serializer);

			// the driver
			tailConfig.setDriver(JoinWithSolutionSetSecondDriver.class);
			tailConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			tailConfig.setDriverComparator(comparator, 0);
			tailConfig.setDriverPairComparator(pairComparator);
			
			tailConfig.setStubWrapper(new UserCodeClassWrapper<UpdateComponentIdMatch>(UpdateComponentIdMatch.class));
		}

		// -- edges ------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edges, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, tail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(tail, fakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		vertices.setVertexToShareInstancesWith(head);
		edges.setVertexToShareInstancesWith(head);

		intermediate.setVertexToShareInstancesWith(head);
		tail.setVertexToShareInstancesWith(head);

		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);
		fakeTail.setVertexToShareInstancesWith(tail);

		return jobGraph;
	}

	public JobGraph createJobGraphSeparateTails(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
		throws JobGraphDefinitionException
	{
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Unified Tails)");

		// input
		JobInputVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		JobInputVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobTaskVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setWaitForSolutionSetUpdate();

		// intermediate
		JobTaskVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobOutputVertex ssFakeTail = createFakeTail(jobGraph, numSubTasks);
		JobOutputVertex wsFakeTail = createFakeTail(jobGraph, numSubTasks);
		JobOutputVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ss join) ----------------------
		JobTaskVertex ssJoinIntermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
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
		JobTaskVertex ssTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationSolutionSetTail",
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
			ssTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			ssTailConfig.setOutputSerializer(serializer);

			// the driver
			ssTailConfig.setDriver(CollectorMapDriver.class);
			ssTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			ssTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// -------------------------- ws tail --------------------------------
		JobTaskVertex wsTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationWorksetTail",
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
			wsTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			wsTailConfig.setOutputSerializer(serializer);

			// the driver
			wsTailConfig.setDriver(CollectorMapDriver.class);
			wsTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			wsTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// --------------- the wiring ---------------------

		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edges, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, ssJoinIntermediate, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		ssJoinIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, ssTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		ssTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, wsTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		wsTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(ssTail, ssFakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(wsTail, wsFakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		vertices.setVertexToShareInstancesWith(head);
		edges.setVertexToShareInstancesWith(head);

		intermediate.setVertexToShareInstancesWith(head);

		ssJoinIntermediate.setVertexToShareInstancesWith(head);
		wsTail.setVertexToShareInstancesWith(head);

		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);

		ssTail.setVertexToShareInstancesWith(wsTail);
		ssFakeTail.setVertexToShareInstancesWith(ssTail);
		wsFakeTail.setVertexToShareInstancesWith(wsTail);

		return jobGraph;
	}

	public JobGraph createJobGraphIntermediateWorksetUpdateAndSolutionSetTail(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
			throws JobGraphDefinitionException {
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Intermediate Workset Update, Solution Set Tail)");

		// input
		JobInputVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		JobInputVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobTaskVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		headConfig.setWaitForSolutionSetUpdate();

		// intermediate
		JobTaskVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobOutputVertex fakeTail = createFakeTail(jobGraph, numSubTasks);
		JobOutputVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ws update) ----------------------
		JobTaskVertex wsUpdateIntermediate =
			JobGraphUtils.createTask(IterationIntermediatePactTask.class, "WorksetUpdate", jobGraph,
				numSubTasks);
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
		JobTaskVertex ssTail =
			JobGraphUtils.createTask(IterationTailPactTask.class, "IterationSolutionSetTail", jobGraph,
				numSubTasks);
		TaskConfig ssTailConfig = new TaskConfig(ssTail.getConfiguration());
		{
			ssTailConfig.setIterationId(ITERATION_ID);
			ssTailConfig.setIsSolutionSetUpdate();
			ssTailConfig.setIsWorksetIteration();

			// inputs and driver
			ssTailConfig.addInputToGroup(0);
			ssTailConfig.setInputSerializer(serializer, 0);

			// output
			ssTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			ssTailConfig.setOutputSerializer(serializer);

			// the driver
			ssTailConfig.setDriver(CollectorMapDriver.class);
			ssTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			ssTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// edges

		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edges, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, wsUpdateIntermediate, ChannelType.NETWORK,
			DistributionPattern.POINTWISE);
		wsUpdateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(wsUpdateIntermediate, ssTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		ssTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(ssTail, fakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		vertices.setVertexToShareInstancesWith(head);
		edges.setVertexToShareInstancesWith(head);

		intermediate.setVertexToShareInstancesWith(head);

		wsUpdateIntermediate.setVertexToShareInstancesWith(head);
		ssTail.setVertexToShareInstancesWith(head);

		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);

		fakeTail.setVertexToShareInstancesWith(ssTail);

		return jobGraph;
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Intermediate solution set update and workset tail
	// -----------------------------------------------------------------------------------------------------------------

	public JobGraph createJobGraphSolutionSetUpdateAndWorksetTail(
			String verticesPath, String edgesPath, String resultPath, int numSubTasks, int maxIterations)
			throws JobGraphDefinitionException {
		// -- init -------------------------------------------------------------------------------------------------
		final TypeSerializerFactory<?> serializer = RecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { LongValue.class }, new boolean[] { true });
		final TypePairComparatorFactory<?, ?> pairComparator = RecordPairComparatorFactory.get();

		JobGraph jobGraph = new JobGraph("Connected Components (Intermediate Solution Set Update, Workset Tail)");

		// input
		JobInputVertex vertices = createVerticesInput(jobGraph, verticesPath, numSubTasks, serializer, comparator);
		JobInputVertex edges = createEdgesInput(jobGraph, edgesPath, numSubTasks, serializer, comparator);

		// head
		JobTaskVertex head = createIterationHead(jobGraph, numSubTasks, serializer, comparator, pairComparator);

		// intermediate
		JobTaskVertex intermediate = createIterationIntermediate(jobGraph, numSubTasks, serializer, comparator);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());

		// output and auxiliaries
		JobOutputVertex output = createOutput(jobGraph, resultPath, numSubTasks, serializer);
		JobOutputVertex fakeTail = createFakeTail(jobGraph, numSubTasks);
		JobOutputVertex sync = createSync(jobGraph, numSubTasks, maxIterations);

		// ------------------ the intermediate (ss update) ----------------------
		JobTaskVertex ssJoinIntermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
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
		JobTaskVertex wsTail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationWorksetTail",
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
			wsTailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			wsTailConfig.setOutputSerializer(serializer);

			// the driver
			wsTailConfig.setDriver(CollectorMapDriver.class);
			wsTailConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			wsTailConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));
		}

		// --------------- the wiring ---------------------

		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edges, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(vertices, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(intermediate, ssJoinIntermediate, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		ssJoinIntermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(ssJoinIntermediate, wsTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);
		wsTailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(wsTail, fakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		vertices.setVertexToShareInstancesWith(head);
		edges.setVertexToShareInstancesWith(head);

		intermediate.setVertexToShareInstancesWith(head);

		ssJoinIntermediate.setVertexToShareInstancesWith(head);
		wsTail.setVertexToShareInstancesWith(head);

		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);

		fakeTail.setVertexToShareInstancesWith(wsTail);

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
