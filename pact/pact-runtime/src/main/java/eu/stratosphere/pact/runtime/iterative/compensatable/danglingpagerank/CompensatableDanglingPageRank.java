/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.playing.JobGraphUtils;
import eu.stratosphere.pact.runtime.iterative.playing.PlayConstants;
import eu.stratosphere.pact.runtime.iterative.playing.pagerank.PageWithRankOutFormat;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationIntermediatePactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.BuildSecondCachedMatchDriver;
import eu.stratosphere.pact.runtime.task.CoGroupDriver;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class CompensatableDanglingPageRank {
	
	private static final TypeSerializerFactory<?> recSerializer = PactRecordSerializerFactory.get();
	
	@SuppressWarnings("unchecked")
	private static final TypeComparatorFactory<?> fieldZeroComparator = new PactRecordComparatorFactory(new int[] {0}, new Class[] {PactLong.class}, new boolean[] {true});
	
	private static final TypePairComparatorFactory<?, ?> pairComparatorFactory = new PactRecordPairComparatorFactory();
	
	private static final int NUM_FILE_HANDLES_PER_SORT = 64;
	
	private static final float SORT_SPILL_THRESHOLD = 0.85f;

	public static void main(String[] args) throws Exception {
		String confPath = args.length >= 6 ? confPath = args[5] : PlayConstants.PLAY_DIR + "local-conf";
		
		GlobalConfiguration.loadConfiguration(confPath);
		Configuration conf = GlobalConfiguration.getConfiguration();
		
		JobGraph jobGraph = getJobGraph(args);
		JobGraphUtils.submit(jobGraph, conf);
	}
		
	public static JobGraph getJobGraph(String[] args) throws Exception {

		int degreeOfParallelism = 2;
		int numSubTasksPerInstance = degreeOfParallelism;
		String pageWithRankInputPath = "file://" + PlayConstants.PLAY_DIR + "test-inputs/danglingpagerank/pageWithRank";
		String adjacencyListInputPath = "file://" + PlayConstants.PLAY_DIR +
			"test-inputs/danglingpagerank/adjacencylists";
		String outputPath = "file:///tmp/stratosphere/iterations";
//		String confPath = PlayConstants.PLAY_DIR + "local-conf";
		int minorConsumer = 25;
		int matchMemory = 50;
		int coGroupSortMemory = 50;
		int numIterations = 25;
		long numVertices = 5;
		long numDanglingVertices = 1;

		String failingWorkers = "1";
		int failingIteration = 2;
		double messageLoss = 0.75;

		if (args.length >= 15) {
			degreeOfParallelism = Integer.parseInt(args[0]);
			numSubTasksPerInstance = Integer.parseInt(args[1]);
			pageWithRankInputPath = args[2];
			adjacencyListInputPath = args[3];
			outputPath = args[4];
//			confPath = args[5];
			minorConsumer = Integer.parseInt(args[6]);
			matchMemory = Integer.parseInt(args[7]);
			coGroupSortMemory = Integer.parseInt(args[8]);
			numIterations = Integer.parseInt(args[9]);
			numVertices = Long.parseLong(args[10]);
			numDanglingVertices = Long.parseLong(args[11]);
			failingWorkers = args[12];
			failingIteration = Integer.parseInt(args[13]);
			messageLoss = Double.parseDouble(args[14]);
		}

		JobGraph jobGraph = new JobGraph("CompensatableDanglingPageRank");
		
		// --------------- the inputs ---------------------

		// page rank input
		JobInputVertex pageWithRankInput = JobGraphUtils.createInput(DanglingPageGenerateRankInputFormat.class,
			pageWithRankInputPath, "DanglingPageWithRankInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig pageWithRankInputConfig = new TaskConfig(pageWithRankInput.getConfiguration());
		pageWithRankInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		pageWithRankInputConfig.setOutputComparator(fieldZeroComparator, 0);
		pageWithRankInputConfig.setOutputSerializer(recSerializer);
		pageWithRankInputConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));

		// edges as adjacency list
		JobInputVertex adjacencyListInput = JobGraphUtils.createInput(AdjacencyListInputFormat.class,
			adjacencyListInputPath, "AdjancencyListInput", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig adjacencyListInputConfig = new TaskConfig(adjacencyListInput.getConfiguration());
		adjacencyListInputConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		adjacencyListInputConfig.setOutputSerializer(recSerializer);
		adjacencyListInputConfig.setOutputComparator(fieldZeroComparator, 0);

		// --------------- the head ---------------------
		JobTaskVertex head = JobGraphUtils.createTask(IterationHeadPactTask.class, "IterationHead", jobGraph,
			degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		
		// initial input / partial solution
		headConfig.addInputToGroup(0);
		headConfig.setIterationHeadPartialSolutionInputIndex(0);
		headConfig.setInputSerializer(recSerializer, 0);
		headConfig.setInputComparator(fieldZeroComparator, 0);
		headConfig.setInputLocalStrategy(0, LocalStrategy.SORT);
		headConfig.setMemoryInput(0, minorConsumer * JobGraphUtils.MEGABYTE);
		headConfig.setFilehandlesInput(0, NUM_FILE_HANDLES_PER_SORT);
		headConfig.setSpillingThresholdInput(0, SORT_SPILL_THRESHOLD);
		
		// back channel / iterations
		headConfig.setBackChannelMemory(minorConsumer * JobGraphUtils.MEGABYTE);
		
		// output into iteration
		headConfig.setOutputSerializer(recSerializer);
		headConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		headConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		
		// final output
		TaskConfig headFinalOutConfig = new TaskConfig(new Configuration());
		headFinalOutConfig.setOutputSerializer(recSerializer);
		headFinalOutConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		headConfig.setIterationHeadFinalOutputConfig(headFinalOutConfig);
		
		// the sync
		headConfig.setIterationHeadIndexOfSyncOutput(3);
		headConfig.setNumberOfIterations(numIterations);
		
		// the driver 
		headConfig.setDriver(MapDriver.class);
		headConfig.setDriverStrategy(DriverStrategy.MAP);
		headConfig.setStubClass(CompensatingMap.class);
		headConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		headConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		headConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		headConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

		// --------------- the join ---------------------
		
		JobTaskVertex intermediate = JobGraphUtils.createTask(IterationIntermediatePactTask.class,
			"IterationIntermediate", jobGraph, degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig intermediateConfig = new TaskConfig(intermediate.getConfiguration());
//		intermediateConfig.setDriver(RepeatableHashjoinMatchDriverWithCachedBuildside.class);
		intermediateConfig.setDriver(BuildSecondCachedMatchDriver.class);
		intermediateConfig.setDriverStrategy(DriverStrategy.HYBRIDHASH_BUILD_SECOND);
		intermediateConfig.setMemoryDriver(matchMemory * JobGraphUtils.MEGABYTE);
		intermediateConfig.addInputToGroup(0);
		intermediateConfig.addInputToGroup(1);
		intermediateConfig.setInputSerializer(recSerializer, 0);
		intermediateConfig.setInputSerializer(recSerializer, 1);
		intermediateConfig.setDriverComparator(fieldZeroComparator, 0);
		intermediateConfig.setDriverComparator(fieldZeroComparator, 1);
		intermediateConfig.setDriverPairComparator(pairComparatorFactory);
		
		intermediateConfig.setOutputSerializer(recSerializer);
		intermediateConfig.addOutputShipStrategy(ShipStrategyType.PARTITION_HASH);
		intermediateConfig.setOutputComparator(fieldZeroComparator, 0);
		
		intermediateConfig.setStubClass(CompensatableDotProductMatch.class);
		intermediateConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		intermediateConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		intermediateConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		intermediateConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));

		// ---------------- the tail (co group) --------------------
		
		JobTaskVertex tail = JobGraphUtils.createTask(IterationTailPactTask.class, "IterationTail", jobGraph,
			degreeOfParallelism, numSubTasksPerInstance);
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		// TODO we need to combine!
		
		// inputs and driver
		tailConfig.setDriver(CoGroupDriver.class);
		tailConfig.setDriverStrategy(DriverStrategy.CO_GROUP);
		tailConfig.addInputToGroup(0);
		tailConfig.addInputToGroup(1);
		tailConfig.setInputSerializer(recSerializer, 0);
		tailConfig.setInputSerializer(recSerializer, 1);
		tailConfig.setDriverComparator(fieldZeroComparator, 0);
		tailConfig.setDriverComparator(fieldZeroComparator, 1);
		tailConfig.setDriverPairComparator(pairComparatorFactory);
		tailConfig.setInputAsynchronouslyMaterialized(0, true);
		tailConfig.setInputMaterializationMemory(0, minorConsumer * JobGraphUtils.MEGABYTE);
		tailConfig.setInputLocalStrategy(1, LocalStrategy.SORT);
		tailConfig.setInputComparator(fieldZeroComparator, 1);
		tailConfig.setMemoryInput(1, coGroupSortMemory * JobGraphUtils.MEGABYTE);
		tailConfig.setFilehandlesInput(1, NUM_FILE_HANDLES_PER_SORT);
		tailConfig.setSpillingThresholdInput(1, SORT_SPILL_THRESHOLD);
		
		// output
		tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		tailConfig.setOutputSerializer(recSerializer);
		
		// the stub
		tailConfig.setStubClass(CompensatableDotProductCoGroup.class);
		tailConfig.setStubParameter("pageRank.numVertices", String.valueOf(numVertices));
		tailConfig.setStubParameter("pageRank.numDanglingVertices", String.valueOf(numDanglingVertices));
		tailConfig.setStubParameter("compensation.failingWorker", failingWorkers);
		tailConfig.setStubParameter("compensation.failingIteration", String.valueOf(failingIteration));
		tailConfig.setStubParameter("compensation.messageLoss", String.valueOf(messageLoss));
		
		// --------------- the output ---------------------

		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "FinalOutput", degreeOfParallelism,
			numSubTasksPerInstance);
		TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
		outputConfig.addInputToGroup(0);
		outputConfig.setInputSerializer(recSerializer, 0);
		outputConfig.setStubClass(PageWithRankOutFormat.class);
		outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);
		
		// --------------- the auxiliaries ---------------------
		
		JobOutputVertex fakeTailOutput = JobGraphUtils.createFakeOutput(jobGraph, "FakeTailOutput",
			degreeOfParallelism, numSubTasksPerInstance);

		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, degreeOfParallelism);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(numIterations);
		syncConfig.setConvergenceCriterion(DiffL1NormConvergenceCriterion.class);
		
		// --------------- the wiring ---------------------

		JobGraphUtils.connect(pageWithRankInput, head, ChannelType.NETWORK, DistributionPattern.BIPARTITE);

		JobGraphUtils.connect(head, intermediate, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		intermediateConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
		
		JobGraphUtils.connect(adjacencyListInput, intermediate, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		
		JobGraphUtils.connect(head, tail, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(intermediate, tail, ChannelType.NETWORK, DistributionPattern.BIPARTITE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, 1);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(1, degreeOfParallelism);

		JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE);
		JobGraphUtils.connect(tail, fakeTailOutput, ChannelType.INMEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);
		
		fakeTailOutput.setVertexToShareInstancesWith(tail);
		tail.setVertexToShareInstancesWith(head);
		pageWithRankInput.setVertexToShareInstancesWith(head);
		adjacencyListInput.setVertexToShareInstancesWith(head);
		intermediate.setVertexToShareInstancesWith(head);
		output.setVertexToShareInstancesWith(head);
		sync.setVertexToShareInstancesWith(head);

		return jobGraph;
	}
}
