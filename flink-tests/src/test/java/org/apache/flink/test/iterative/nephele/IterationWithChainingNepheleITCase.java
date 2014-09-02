/**
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

import java.util.Collection;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.FileOutputFormat;
import org.apache.flink.api.java.typeutils.runtime.record.RecordComparatorFactory;
import org.apache.flink.api.java.typeutils.runtime.record.RecordSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.iterative.task.IterationHeadPactTask;
import org.apache.flink.runtime.iterative.task.IterationTailPactTask;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphDefinitionException;
import org.apache.flink.runtime.jobgraph.JobInputVertex;
import org.apache.flink.runtime.jobgraph.JobOutputVertex;
import org.apache.flink.runtime.jobgraph.JobTaskVertex;
import org.apache.flink.runtime.operators.CollectorMapDriver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.GroupReduceDriver;
import org.apache.flink.runtime.operators.chaining.ChainedCollectorMapDriver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.test.iterative.IterationWithChainingITCase;
import org.apache.flink.test.recordJobs.kmeans.udfs.CoordVector;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointInFormat;
import org.apache.flink.test.recordJobs.kmeans.udfs.PointOutFormat;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests chained iteration tails.
 * <p/>
 * GitHub issue #123 reports a problem with chaining of tasks to iteration tails. The initial fix worked around the
 * issue by having the compiler *not* chain tasks to an iteration tail. The existing IterationWithChainingITCase only
 * tests this compiler behavior. The JobGraph and bypasses the compiler to test the original chaining problem.
 * <p/>
 * A chained mapper after the iteration tail (dummy reduce) increments the given input points in each iteration. The
 * final result will only be correct, if the chained mapper is successfully executed.
 * 
 * {@link IterationWithChainingITCase}
 */
@RunWith(Parameterized.class)
public class IterationWithChainingNepheleITCase extends RecordAPITestBase {

	private static final String INPUT_STRING = "0|%d.25|\n" + "1|%d.25|\n";

	private String dataPath;

	private String resultPath;

	public IterationWithChainingNepheleITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(DOP);
	}

	@Override
	protected void preSubmit() throws Exception {
		String initialInput = String.format(INPUT_STRING, 1, 2);
		dataPath = createTempFile("data_points.txt", initialInput);
		resultPath = getTempFilePath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		int maxIterations = config.getInteger("ChainedMapperNepheleITCase#MaxIterations", 1);
		String result = String.format(INPUT_STRING, 1 + maxIterations, 2 + maxIterations);
		compareResultsByLinesInMemory(result, resultPath);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();
		config.setInteger("ChainedMapperNepheleITCase#NoSubtasks", DOP);
		config.setInteger("ChainedMapperNepheleITCase#MaxIterations", 2);
		return toParameterList(config);
	}

	@Override
	protected JobGraph getJobGraph() throws Exception {
		int numSubTasks = config.getInteger("ChainedMapperNepheleITCase#NoSubtasks", 1);
		int maxIterations = config.getInteger("ChainedMapperNepheleITCase#MaxIterations", 1);

		return getTestJobGraph(dataPath, resultPath, numSubTasks, maxIterations);
	}

	private JobGraph getTestJobGraph(String inputPath, String outputPath, int numSubTasks, int maxIterations)
			throws JobGraphDefinitionException {

		final JobGraph jobGraph = new JobGraph("Iteration Tail with Chaining");

		final TypeSerializerFactory<Record> serializer = RecordSerializerFactory.get();

		@SuppressWarnings("unchecked")
		final TypeComparatorFactory<Record> comparator =
			new RecordComparatorFactory(new int[] { 0 }, new Class[] { IntValue.class });

		final int ITERATION_ID = 1;

		// --------------------------------------------------------------------------------------------------------------
		// 1. VERTICES
		// --------------------------------------------------------------------------------------------------------------

		// - input -----------------------------------------------------------------------------------------------------
		JobInputVertex input = JobGraphUtils.createInput(
			new PointInFormat(), inputPath, "Input", jobGraph, numSubTasks);
		TaskConfig inputConfig = new TaskConfig(input.getConfiguration());
		{
			inputConfig.setOutputSerializer(serializer);
			inputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
		}

		// - head ------------------------------------------------------------------------------------------------------
		JobTaskVertex head = JobGraphUtils.createTask(
			IterationHeadPactTask.class, "Iteration Head", jobGraph, numSubTasks);
		TaskConfig headConfig = new TaskConfig(head.getConfiguration());
		{
			headConfig.setIterationId(ITERATION_ID);

			// input to iteration head
			headConfig.addInputToGroup(0);
			headConfig.setInputSerializer(serializer, 0);
			headConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			headConfig.setIterationHeadPartialSolutionOrWorksetInputIndex(0);

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

			// driver
			headConfig.setDriver(CollectorMapDriver.class);
			headConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			headConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));

			// back channel
			headConfig.setRelativeBackChannelMemory(1.0);
		}

		// - tail ------------------------------------------------------------------------------------------------------
		JobTaskVertex tail = JobGraphUtils.createTask(
			IterationTailPactTask.class, "Chained Iteration Tail", jobGraph, numSubTasks);
		TaskConfig tailConfig = new TaskConfig(tail.getConfiguration());
		{
			tailConfig.setIterationId(ITERATION_ID);

			// inputs and driver
			tailConfig.addInputToGroup(0);
			tailConfig.setInputSerializer(serializer, 0);

			// output
			tailConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			tailConfig.setOutputSerializer(serializer);

			// the driver
			tailConfig.setDriver(GroupReduceDriver.class);
			tailConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP_REDUCE);
			tailConfig.setDriverComparator(comparator, 0);
			tailConfig.setStubWrapper(new UserCodeClassWrapper<DummyReducer>(DummyReducer.class));

			// chained mapper
			TaskConfig chainedMapperConfig = new TaskConfig(new Configuration());
			chainedMapperConfig.setDriverStrategy(DriverStrategy.COLLECTOR_MAP);
			chainedMapperConfig.setStubWrapper(new UserCodeClassWrapper<IncrementCoordinatesMapper>(
				IncrementCoordinatesMapper.class));

			chainedMapperConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
			chainedMapperConfig.setInputSerializer(serializer, 0);

			chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
			chainedMapperConfig.setOutputSerializer(serializer);

			chainedMapperConfig.setIsWorksetUpdate();

			tailConfig.addChainedTask(ChainedCollectorMapDriver.class, chainedMapperConfig, "Chained ID Mapper");
		}

		// - output ----------------------------------------------------------------------------------------------------
		JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks);
		TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
		{
			outputConfig.addInputToGroup(0);
			outputConfig.setInputSerializer(serializer, 0);

			outputConfig.setStubWrapper(new UserCodeClassWrapper<PointOutFormat>(PointOutFormat.class));
			outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);
		}

		// - fake tail -------------------------------------------------------------------------------------------------
		JobOutputVertex fakeTail = JobGraphUtils.createFakeOutput(jobGraph, "Fake Tail", numSubTasks);

		// - sync ------------------------------------------------------------------------------------------------------
		JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, numSubTasks);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setNumberOfIterations(maxIterations);
		syncConfig.setIterationId(ITERATION_ID);

		// --------------------------------------------------------------------------------------------------------------
		// 2. EDGES
		// --------------------------------------------------------------------------------------------------------------
		JobGraphUtils.connect(input, head, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, tail, ChannelType.IN_MEMORY, DistributionPattern.BIPARTITE);
		tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

		JobGraphUtils.connect(head, output, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

		JobGraphUtils.connect(tail, fakeTail, ChannelType.IN_MEMORY, DistributionPattern.POINTWISE);

		// --------------------------------------------------------------------------------------------------------------
		// 3. INSTANCE SHARING
		// --------------------------------------------------------------------------------------------------------------
		input.setVertexToShareInstancesWith(head);

		tail.setVertexToShareInstancesWith(head);

		output.setVertexToShareInstancesWith(head);

		sync.setVertexToShareInstancesWith(head);

		fakeTail.setVertexToShareInstancesWith(tail);

		return jobGraph;
	}

	public static final class DummyMapper extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record rec, Collector<Record> out) {
			out.collect(rec);
		}
	}

	public static final class DummyReducer implements GroupReduceFunction<Record, Record> {

		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterable<Record> it, Collector<Record> out) {
			for (Record r :it) {
				out.collect(r);
			}
		}
	}

	public static final class IncrementCoordinatesMapper extends MapFunction {

		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record rec, Collector<Record> out) {
			CoordVector coord = rec.getField(1, CoordVector.class);

			double[] vector = coord.getCoordinates();
			for (int i = 0; i < vector.length; i++) {
				vector[i]++;
			}

			rec.setField(1, coord);
			out.collect(rec);
		}
	}
}
