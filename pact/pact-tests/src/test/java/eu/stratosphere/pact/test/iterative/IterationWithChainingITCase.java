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
package eu.stratosphere.pact.test.iterative;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.example.kmeans.udfs.PointInFormat;
import eu.stratosphere.pact.example.kmeans.udfs.PointOutFormat;
import eu.stratosphere.pact.generic.contract.BulkIteration;
import eu.stratosphere.pact.test.util.TestBase2;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

@RunWith(Parameterized.class)
public class IterationWithChainingITCase extends TestBase2 {

    private static final String DATA_POINTS = "0|50.90|16.20|72.08|\n" + "1|73.65|61.76|62.89|\n" + "2|61.73|49.95|92.74|\n";

    private String dataPath;
    private String resultPath;

    public IterationWithChainingITCase(Configuration config) {
        super(config);
    }

    @Override
    protected void preSubmit() throws Exception {
        dataPath = createTempFile("data_points.txt", DATA_POINTS);
        resultPath = getTempFilePath("result");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(DATA_POINTS, resultPath);
    }


    @Override
    protected Plan getPactPlan() {
        Plan plan = getTestPlan(config.getInteger("ChainedMapperITCase#NoSubtasks", 1), dataPath, resultPath);
        return plan;
    }

    @Parameters
    public static Collection<Object[]> getConfigurations() {
        Configuration config1 = new Configuration();
        config1.setInteger("ChainedMapperITCase#NoSubtasks", 4);
        return toParameterList(config1);
    }

    public static final class IdentityMapper extends MapStub implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void map(PactRecord rec, Collector<PactRecord> out) {
            out.collect(rec);
        }
    }

    public static final class DummyReducer extends ReduceStub implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void reduce(Iterator<PactRecord> it, Collector<PactRecord> out) {
            while (it.hasNext()) {
                out.collect(it.next());
            }
        }
    }

    static Plan getTestPlan(int numSubTasks, String input, String output) {

        FileDataSource initialInput = new FileDataSource(new PointInFormat(), input, "Input");
        initialInput.setDegreeOfParallelism(1);

        BulkIteration iteration = new BulkIteration("Loop");
        iteration.setInput(initialInput);
        iteration.setMaximumNumberOfIterations(2);

        ReduceContract dummyReduce = ReduceContract.builder(new DummyReducer(), PactInteger.class, 0)
                .input(iteration.getPartialSolution())
                .name("Reduce something")
                .build();


        MapContract dummyMap = MapContract.builder(new IdentityMapper()).input(dummyReduce).build();
        iteration.setNextPartialSolution(dummyMap);

        FileDataSink finalResult = new FileDataSink(new PointOutFormat(), output, iteration, "Output");

        Plan plan = new Plan(finalResult, "Iteration with chained map test");
        plan.setDefaultParallelism(numSubTasks);
        return plan;
    }
}
