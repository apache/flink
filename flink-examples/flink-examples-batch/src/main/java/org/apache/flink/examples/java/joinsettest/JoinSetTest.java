package org.apache.flink.examples.java.joinsettest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * Implements the "JoinTest" program to test the performance of join operator in batch mode and stream mode.
 *
 * <p>Usage: <code>JoinTest --batch &lt; --dataNum &lt;</code>
 * If no parameters are provided, the program is run with 5e5 records in stream mode.
 */
public class JoinSetTest {
    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        final ExecutionEnvironment env =
                ExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setAutoWatermarkInterval(0);
        env.setParallelism(1);
        if (params.has("parallelism")) {
            System.out.println("Parallelism set to: " + params.get("parallelism"));
            env.setParallelism(Integer.parseInt(params.get("parallelism")));
        }

        // get input data
        long dataNum = (long) 5e5;
        if (params.has("dataNum")) {
            dataNum = Long.parseLong(params.get("dataNum"));
            System.out.println("Number of records: " + dataNum);
        }
        DataSet<Integer> ds1 =
                env.fromCollection(
                        new DataGenerator(dataNum, System.currentTimeMillis()),
                        Types.INT);
        DataSet<Integer> ds2 =
                env.fromCollection(
                        new DataGenerator(dataNum, System.currentTimeMillis()),
                        Types.INT);

        ds1.coGroup(ds2)
                .where(Integer::intValue)
                .equalTo(Integer::intValue)
                .with(new CoGroupFunction<Integer, Integer, Integer>() {
                    @Override
                    public void coGroup(
                            Iterable<Integer> first,
                            Iterable<Integer> second,
                            Collector<Integer> out) throws Exception {
                        out.collect(1);
                    }
//                    @Override
//                    public Integer (Integer first, Integer second) throws Exception {
//                        return 1;
//                    }

//                    @Override
//                    public void join(
//                            Integer first,
//                            Integer second,
//                            Collector<Integer> out) throws Exception {
////                        System.out.println(first + " " + second);
//                        out.collect(1);
//                    }
                }).write(new CountingAndDiscardingSink(), "/tmp");

        long startTime = System.currentTimeMillis();
        JobExecutionResult executionResult = env.execute("Flink Benchmark Job");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
        System.out.println("Throughput: " + dataNum / (endTime - startTime));
        long count = executionResult.getAccumulatorResult(CountingAndDiscardingSink.COUNTER_NAME);
        System.out.println(count);
    }

    /**
     * A set sink that counts the number of all elements. The counting result is stored in an
     * {@link org.apache.flink.api.common.accumulators.Accumulator} specified by {@link
     * #COUNTER_NAME} and can be acquired by {@link
     * org.apache.flink.api.common.JobExecutionResult#getAccumulatorResult(String)}.
     */
    private static class CountingAndDiscardingSink extends FileOutputFormat<Integer> {

        public static final String COUNTER_NAME = "numElements";
        private static final long serialVersionUID = 1L;
        private final LongCounter numElementsCounter = new LongCounter();

        @Override
        public void open(InitializationContext context) throws IOException {
            getRuntimeContext().addAccumulator(COUNTER_NAME, numElementsCounter);
        }

        @Override
        public void writeRecord(Integer record) throws IOException {
            numElementsCounter.add(1L);
        }
    }

    private static class DataGenerator
            implements Iterator<Integer>, Serializable {
        private Random random;
        private final long numValues;
        private int numPreGeneratedData;
        private Integer[] preGeneratedData;
        int cnt = 0;

        DataGenerator(long numValues, long initSeed) {
            this.numValues = numValues;
            random = new Random(initSeed);
            numPreGeneratedData = Math.max((int) numValues / 1000, 100);

            preGeneratedData = new Integer[numPreGeneratedData];
            for (int i = 0; i < numPreGeneratedData; i++) {
                preGeneratedData[i] = random.nextInt(numPreGeneratedData);
            }
        }

        @Override
        public boolean hasNext() {
            return cnt < numValues;
        }

        @Override
        public Integer next() {
            cnt += 1;
            return preGeneratedData[cnt % numPreGeneratedData];
        }
    }
}
