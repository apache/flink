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

package eu.stratosphere.pact.test.iterative.nephele;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Random;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.aggregators.LongSumAggregator;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.example.connectedcomponents.WorksetConnectedComponents.MinimumComponentIDReduce;
import eu.stratosphere.pact.example.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.pact.example.connectedcomponents.WorksetConnectedComponents.UpdateComponentIdMatch;
import eu.stratosphere.pact.generic.contract.UserCodeClassWrapper;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.BuildSecondCachedMatchDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.JoinWithSolutionSetMatchDriver.SolutionSetSecondJoinDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.test.util.TestBase2;

@RunWith(Parameterized.class)
public class ConnectedComponentsNepheleITCase extends TestBase2 {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;
	
	
	public ConnectedComponentsNepheleITCase(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected JobGraph getJobGraph() throws Exception {
		int dop = config.getInteger("ConnectedComponentsNephele#NumSubtasks", 1);
		int maxIterations = config.getInteger("ConnectedComponentsNephele#NumIterations", 1);
		
		return createWorksetConnectedComponentsJobGraph(verticesPath, edgesPath, resultPath, dop, maxIterations);
	}
	
//	@Override
//	protected Plan getPactPlan() {
//		int dop = config.getInteger("ConnectedComponentsNephele#NumSubtasks", 1);
//		int maxIterations = config.getInteger("ConnectedComponentsNephele#NumIterations", 1);
//		String[] params = { String.valueOf(dop) , verticesPath, edgesPath, resultPath, String.valueOf(maxIterations) };
//		
//		WorksetConnectedComponents cc = new WorksetConnectedComponents();
//		return cc.getPlan(params);
//	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			checkOddEvenResult(reader);
		}
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("ConnectedComponentsNephele#NumSubtasks", 4);
		config1.setInteger("ConnectedComponentsNephele#NumIterations", 100);
		return toParameterList(config1);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private JobGraph createWorksetConnectedComponentsJobGraph(String verticesPath, String edgesPath, 
			String resultPath, int degreeOfParallelism, int maxIterations) throws JobGraphDefinitionException
	{
		final int numSubTasksPerInstance = degreeOfParallelism;
		
		final TypeSerializerFactory<?> serializer = PactRecordSerializerFactory.get();
		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<?> comparator = new PactRecordComparatorFactory(new int[] {0}, new Class[] {PactLong.class}, new boolean[] {true});
		final TypePairComparatorFactory<?, ?> pairComparator = PactRecordPairComparatorFactory.get();
		
		final long MEM_PER_CONSUMER = 10;
		final int ITERATION_ID = 1;
		
		
		JobGraph jobGraph = new JobGraph("Connected Components");
		
		// --------------- the inputs ---------------------

		// vertices
		@SuppressWarnings("unchecked")
		RecordInputFormat verticesInFormat = new RecordInputFormat(' ', PactLong.class);
		JobInputVertex verticesInput = JobGraphUtils.createInput(verticesInFormat,
			verticesPath, "VerticesInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig verticesInputConfig = new TaskConfig(verticesInput.getConfiguration());
			verticesInputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			verticesInputConfig.setOutputSerializer(serializer);
			
			
			// chained mapper that duplicates the id
			TaskConfig chainedMapperConfig = new TaskConfig(new Configuration());
			chainedMapperConfig.setStubWrapper(new UserCodeClassWrapper<IdDuplicator>(IdDuplicator.class));
			chainedMapperConfig.setDriverStrategy(DriverStrategy.MAP);
			chainedMapperConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			chainedMapperConfig.setInputSerializer(serializer, 0);
			
			chainedMapperConfig.setOutputSerializer(serializer);
			chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			chainedMapperConfig.setOutputComparator(comparator, 0);
			chainedMapperConfig.setOutputComparator(comparator, 1);
			
			verticesInputConfig.addChainedTask(ChainedMapDriver.class, chainedMapperConfig, "ID Duplicator");
		}

		// edges 
		@SuppressWarnings("unchecked")
		RecordInputFormat edgesInFormat = new RecordInputFormat(' ', PactLong.class, PactLong.class);
		JobInputVertex edgeInput = JobGraphUtils.createInput(edgesInFormat,
			edgesPath, "EdgesInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig edgesInputConfig = new TaskConfig(edgeInput.getConfiguration());
			edgesInputConfig.setOutputSerializer(serializer);
			edgesInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
			edgesInputConfig.setOutputComparator(comparator, 0);
		}
		
		// --------------- the iteration head ---------------------
		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "Join With Edges (Iteration Head)", jobGraph,
			degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig headConfig = new TaskConfig(head.getConfiguration());
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
			headConfig.setInputMaterializationMemory(1, MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
			// initial solution set input
			headConfig.addInputToGroup(2);
			headConfig.setInputSerializer(serializer, 2);
			headConfig.setInputComparator(comparator, 2);
			headConfig.setInputLocalStrategy(2, LocalStrategy.NONE);
			headConfig.setIterationHeadSolutionSetInputIndex(2);
			
			headConfig.setSolutionSetSerializer(serializer);
			headConfig.setSolutionSetComparator(comparator);
			headConfig.setSolutionSetProberSerializer(serializer);
			headConfig.setSolutionSetProberComparator(comparator);
			headConfig.setSolutionSetPairComparator(pairComparator);
			
			// back channel / iterations
			headConfig.setWorksetIteration();
			headConfig.setBackChannelMemory(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			headConfig.setSolutionSetMemory(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
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
			headConfig.setStubWrapper(new UserCodeClassWrapper<NeighborWithComponentIDJoin>(NeighborWithComponentIDJoin.class));
			headConfig.setDriverComparator(comparator, 0);
			headConfig.setDriverComparator(comparator, 1);
			headConfig.setDriverPairComparator(pairComparator);
			headConfig.setMemoryDriver(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			
			headConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		}
		
		// --------------- the intermediate (reduce to min id) ---------------
		JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"Find Min Component-ID", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
		{
			intermediateConfig.setIterationId(ITERATION_ID);
		
			intermediateConfig.addInputToGroup(0);
			intermediateConfig.setInputSerializer(serializer, 0);
			intermediateConfig.setInputComparator(comparator, 0);
			intermediateConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
			intermediateConfig.setMemoryInput(0, MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
			intermediateConfig.setFilehandlesInput(0, 64);
			intermediateConfig.setSpillingThresholdInput(0, 0.85f);
		
			intermediateConfig.setOutputSerializer(serializer);
			intermediateConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		
			intermediateConfig.setDriver(ReduceDriver.class);
			intermediateConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP);
			intermediateConfig.setDriverComparator(comparator, 0);
			intermediateConfig.setStubWrapper(new UserCodeClassWrapper<MinimumComponentIDReduce>(MinimumComponentIDReduce.class));
		}
		
		// --------------- the tail (solution set join) ---------------
		JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
			degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		{
			tailConfig.setIterationId(ITERATION_ID);
			tailConfig.setWorksetIteration();
			tailConfig.setUpdateSolutionSet();
			tailConfig.setUpdateSolutionSetWithoutReprobe();
		
			// inputs and driver
			tailConfig.addInputToGroup(0);
			tailConfig.setInputSerializer(serializer, 0);
			
			// output
			tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			tailConfig.setOutputSerializer(serializer);
		
			// the driver
			tailConfig.setDriver(SolutionSetSecondJoinDriver.class);
			tailConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
			tailConfig.setStubWrapper(new UserCodeClassWrapper<UpdateComponentIdMatch>(UpdateComponentIdMatch.class));
			tailConfig.setSolutionSetSerializer(serializer);
		}
		
		// --------------- the output ---------------------
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Final Output", degreeOfParallelism, numSubTasksPerInstance);
		{
			TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
			
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(serializer, 0);
			
			outputConfig.setStubWrapper(new UserCodeClassWrapper<RecordOutputFormat>(RecordOutputFormat.class));
			outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, resultPath);
			
			Configuration outputUserConfig = outputConfig.getStubParameters();
			outputUserConfig.setString(RecordOutputFormat.RECORD_DELIMITER_PARAMETER, "\n");
			outputUserConfig.setString(RecordOutputFormat.FIELD_DELIMITER_PARAMETER, " ");
			outputUserConfig.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 0, PactLong.class);
			outputUserConfig.setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 0, 0);
			outputUserConfig.setClass(RecordOutputFormat.FIELD_TYPE_PARAMETER_PREFIX + 1, PactLong.class);
			outputUserConfig.setInteger(RecordOutputFormat.RECORD_POSITION_PARAMETER_PREFIX + 1, 1);
			outputUserConfig.setInteger(RecordOutputFormat.NUM_FIELDS_PARAMETER, 2);
		}
		
		// --------------- the auxiliaries ---------------------
		
		JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput",
			degreeOfParallelism, numSubTasksPerInstance);

		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(maxIterations);
		syncConfig.setIterationId(ITERATION_ID);
		syncConfig.addIterationAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, LongSumAggregator.class);
		syncConfig.setConvergenceCriterion(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME, WorksetEmptyConvergenceCriterion.class);
		
		// --------------- the wiring ---------------------

		JobGraphUtils.connect(verticesInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(edgeInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		JobGraphUtils.connect(verticesInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
		
		JobGraphUtils.connect(intermediate, tail, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);

		JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		verticesInput.setVertexToShareInstancesWith(head);
		edgeInput.setVertexToShareInstancesWith(head);
		
		intermediate.setVertexToShareInstancesWith(head);
		tail.setVertexToShareInstancesWith(head);
		
		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);
		fakeTailOutput.setVertexToShareInstancesWith(tail);
		
		return jobGraph;
	}
	
	// --------------------------------------------------------------------------------------------
	// --------------------------------------------------------------------------------------------
	
	public static final String getEnumeratingVertices(int num) {
		if (num < 1 || num > 1000000)
			throw new IllegalArgumentException();
		
		StringBuilder bld = new StringBuilder(3 * num);
		for (int i = 1; i <= num; i++) {
			bld.append(i);
			bld.append('\n');
		}
		return bld.toString();
	}
	
	/**
	 * Creates random edges such that even numbered vertices are connected with even numbered vertices
	 * and odd numbered vertices only with other odd numbered ones.
	 * 
	 * @param numEdges
	 * @param numVertices
	 * @param seed
	 * @return
	 */
	public static final String getRandomOddEvenEdges(int numEdges, int numVertices, long seed) {
		if (numVertices < 2 || numVertices > 1000000 || numEdges < numVertices || numEdges > 1000000)
			throw new IllegalArgumentException();
		
		StringBuilder bld = new StringBuilder(5 * numEdges);
		
		// first create the linear edge sequence even -> even and odd -> odd to make sure they are
		// all in the same component
		for (int i = 3; i <= numVertices; i++) {
			bld.append(i-2).append(' ').append(i).append('\n');
		}
		
		numEdges -= numVertices - 2;
		Random r = new Random(seed);
		
		for (int i = 1; i <= numEdges; i++) {
			int evenOdd = r.nextBoolean() ? 1 : 0;
			
			int source = r.nextInt(numVertices) + 1;
			if (source % 2 != evenOdd) {
				source--;
				if (source < 1) {
					source = 2;
				}
			}
			
			int target = r.nextInt(numVertices) + 1;
			if (target % 2 != evenOdd) {
				target--;
				if (target < 1) {
					target = 2;
				}
			}
			
			bld.append(source).append(' ').append(target).append('\n');
		}
		return bld.toString();
	}
	
	public static void checkOddEvenResult(BufferedReader result) throws IOException {
		Pattern split = Pattern.compile(" ");
		String line;
		while ((line = result.readLine()) != null) {
			String[] res = split.split(line);
			Assert.assertEquals("Malfored result: Wrong number of tokens in line.", 2, res.length);
			try {
				int vertex = Integer.parseInt(res[0]);
				int component = Integer.parseInt(res[1]);
				
				int should = vertex % 2;
				if (should == 0) {
					should = 2;
				}
				Assert.assertEquals("Vertex is in wrong component.", should, component);
			}
			catch (NumberFormatException e) {
				Assert.fail("Malformed result.");
			}
		}
	}
	
	public static final class IdDuplicator extends MapStub {

		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			record.setField(1, record.getField(0, PactLong.class));
			out.collect(record);
		}
		
	}
}
