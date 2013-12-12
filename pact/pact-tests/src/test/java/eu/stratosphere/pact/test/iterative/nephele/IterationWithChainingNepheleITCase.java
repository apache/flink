/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.*;
import eu.stratosphere.pact.common.io.FileOutputFormat;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.kmeans.udfs.CoordVector;
import eu.stratosphere.pact.example.kmeans.udfs.PointInFormat;
import eu.stratosphere.pact.example.kmeans.udfs.PointOutFormat;
import eu.stratosphere.pact.generic.contract.UserCodeClassWrapper;
import eu.stratosphere.pact.generic.types.TypeComparatorFactory;
import eu.stratosphere.pact.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.MapDriver;
import eu.stratosphere.pact.runtime.task.ReduceDriver;
import eu.stratosphere.pact.runtime.task.chaining.ChainedMapDriver;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.test.util.TestBase2;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Iterator;

/**
 * Tests chained iteration tails.
 * <p/>
 * GitHub issue #123 reports a problem with chaining of tasks to iteration tails. The initial fix worked around the
 * issue by having the compiler *not* chain tasks to an iteration tail. The existing IterationWithChainingITCase only
 * tests this compiler behavior. The JobGraph and bypasses the compiler to test the original
 * chaining problem.
 * <p/>
 * A chained mapper after the iteration tail (dummy reduce) increments the given input points in each iteration. The
 * final result will only be correct, if the chained mapper is successfully executed.
 *
 * @link {eu.stratosphere.pact.test.iterative.IterationWithChainingITCase}
 * @link {https://github.com/stratosphere/stratosphere/issues/123}
 */
@RunWith(Parameterized.class)
public class IterationWithChainingNepheleITCase extends TestBase2 {

    private static final String INPUT_STRING = "0|%d.25|\n" + "1|%d.25|\n";

    private String dataPath;
    private String resultPath;

    public IterationWithChainingNepheleITCase(Configuration config) {
        super(config);
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
        config.setInteger("ChainedMapperNepheleITCase#NoSubtasks", 2);
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

        final TypeSerializerFactory<PactRecord> serializer = PactRecordSerializerFactory.get();

        @SuppressWarnings("unchecked")
		final TypeComparatorFactory<PactRecord> comparator =
                new PactRecordComparatorFactory(new int[]{0}, new Class[]{PactInteger.class});

        final long MEM_PER_CONSUMER = 10;

        final int ITERATION_ID = 1;

        //--------------------------------------------------------------------------------------------------------------
        // 1. VERTICES
        //--------------------------------------------------------------------------------------------------------------

        // - input -----------------------------------------------------------------------------------------------------
        JobInputVertex input = JobGraphUtils.createInput(
                new PointInFormat(), inputPath, "Input", jobGraph, numSubTasks, numSubTasks);
        TaskConfig inputConfig = new TaskConfig(input.getConfiguration());
        {
            inputConfig.setOutputSerializer(serializer);
            inputConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
        }

        // - head ------------------------------------------------------------------------------------------------------
        JobTaskVertex head = JobGraphUtils.createTask(
                IterationHeadPactTask.class, "Iteration Head", jobGraph, numSubTasks, numSubTasks);
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
            headConfig.setDriver(MapDriver.class);
            headConfig.setDriverStrategy(DriverStrategy.MAP);
            headConfig.setStubWrapper(new UserCodeClassWrapper<DummyMapper>(DummyMapper.class));

            // back channel
            headConfig.setBackChannelMemory(MEM_PER_CONSUMER * JobGraphUtils.MEGABYTE);
        }

        // - tail ------------------------------------------------------------------------------------------------------
        JobTaskVertex tail = JobGraphUtils.createTask(
                IterationTailPactTask.class, "Chained Iteration Tail", jobGraph, numSubTasks, numSubTasks);
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
            tailConfig.setDriver(ReduceDriver.class);
            tailConfig.setDriverStrategy(DriverStrategy.SORTED_GROUP);
            tailConfig.setDriverComparator(comparator, 0);
            tailConfig.setStubWrapper(new UserCodeClassWrapper<DummyReducer>(DummyReducer.class));

            // chained mapper
            TaskConfig chainedMapperConfig = new TaskConfig(new Configuration());
            chainedMapperConfig.setDriverStrategy(DriverStrategy.MAP);
            chainedMapperConfig.setStubWrapper(new UserCodeClassWrapper<IncrementCoordinatesMapper>(IncrementCoordinatesMapper.class));

            chainedMapperConfig.setInputLocalStrategy(0, LocalStrategy.NONE);
            chainedMapperConfig.setInputSerializer(serializer, 0);

            chainedMapperConfig.addOutputShipStrategy(ShipStrategyType.FORWARD);
            chainedMapperConfig.setOutputSerializer(serializer);
            
            chainedMapperConfig.setIsWorksetUpdate();

            tailConfig.addChainedTask(ChainedMapDriver.class, chainedMapperConfig, "Chained ID Mapper");
        }

        // - output ----------------------------------------------------------------------------------------------------
        JobOutputVertex output = JobGraphUtils.createFileOutput(jobGraph, "Output", numSubTasks, numSubTasks);
        TaskConfig outputConfig = new TaskConfig(output.getConfiguration());
        {
            outputConfig.addInputToGroup(0);
            outputConfig.setInputSerializer(serializer, 0);

            outputConfig.setStubWrapper(new UserCodeClassWrapper<PointOutFormat>(PointOutFormat.class));
            outputConfig.setStubParameter(FileOutputFormat.FILE_PARAMETER_KEY, outputPath);
        }

        // - fake tail -------------------------------------------------------------------------------------------------
        JobOutputVertex fakeTail = JobGraphUtils.createFakeOutput(jobGraph, "Fake Tail", numSubTasks, numSubTasks);

        // - sync ------------------------------------------------------------------------------------------------------
        JobOutputVertex sync = JobGraphUtils.createSync(jobGraph, numSubTasks);
        TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
        syncConfig.setNumberOfIterations(maxIterations);
        syncConfig.setIterationId(ITERATION_ID);

        //--------------------------------------------------------------------------------------------------------------
        // 2. EDGES
        //--------------------------------------------------------------------------------------------------------------
        JobGraphUtils.connect(input, head, ChannelType.INMEMORY, DistributionPattern.POINTWISE);

        JobGraphUtils.connect(head, tail, ChannelType.INMEMORY, DistributionPattern.BIPARTITE);
        tailConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, numSubTasks);

        JobGraphUtils.connect(head, output, ChannelType.INMEMORY, DistributionPattern.POINTWISE);

        JobGraphUtils.connect(head, sync, ChannelType.NETWORK, DistributionPattern.POINTWISE);

        JobGraphUtils.connect(tail, fakeTail, ChannelType.INMEMORY, DistributionPattern.POINTWISE);

        //--------------------------------------------------------------------------------------------------------------
        // 3. INSTANCE SHARING
        //--------------------------------------------------------------------------------------------------------------
        input.setVertexToShareInstancesWith(head);

        tail.setVertexToShareInstancesWith(head);

        output.setVertexToShareInstancesWith(head);

        sync.setVertexToShareInstancesWith(head);

        fakeTail.setVertexToShareInstancesWith(tail);

        return jobGraph;
    }

    public static final class DummyMapper extends MapStub {
        @Override
        public void map(PactRecord rec, Collector<PactRecord> out) {
            out.collect(rec);
        }
    }

    public static final class DummyReducer extends ReduceStub {
        @Override
        public void reduce(Iterator<PactRecord> it, Collector<PactRecord> out) {
            while (it.hasNext()) {
                out.collect(it.next());
            }
        }
    }

    public static final class IncrementCoordinatesMapper extends MapStub {
        @Override
        public void map(PactRecord rec, Collector<PactRecord> out) {
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
