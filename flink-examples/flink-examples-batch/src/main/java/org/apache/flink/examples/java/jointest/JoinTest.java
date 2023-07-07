package org.apache.flink.examples.java.jointest;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.examples.java.jointest.util.EndOfStreamWindow;
import org.apache.flink.examples.java.jointest.util.MyJoinSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * Implements the "JoinTest" program to test the performance of join operator in batch mode and
 * stream mode.
 *
 * <p>Usage: <code>JoinTest --batch &lt; --dataNum &lt;</code> If no parameters are provided, the
 * program is run with 5e5 records in stream mode.
 */
public class JoinTest {
    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        Configuration config = new Configuration();
        config.setBoolean(DeploymentOptions.ATTACHED, true);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(0);
        env.getCheckpointConfig().disableCheckpointing();
        if (params.has("parallelism")) {
            System.out.println("Parallelism set to: " + params.get("parallelism"));
            env.setParallelism(Integer.parseInt(params.get("parallelism")));
        }
        if (params.has("batch")) {
            System.out.println("Running in batch mode");
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }

        // get input data
        long dataNum = (long) 5e5;
        if (params.has("dataNum")) {
            dataNum = Long.parseLong(params.get("dataNum"));
            System.out.println("Number of records: " + dataNum);
        }
        DataStream<Integer> ds1 =
                env.addSource(new MyJoinSource(dataNum, System.currentTimeMillis()));
        DataStream<Integer> ds2 =
                env.addSource(new MyJoinSource(dataNum, System.currentTimeMillis()));

        //        DataGeneratorSource<String> s1 = new DataGeneratorSource<>(new
        // MyJoinGenerator((long) 1e6, System.currentTimeMillis()), (long) 1e6, Types.STRING);
        //        DataGeneratorSource<String> s2 = new DataGeneratorSource<>(new
        // MyJoinGenerator((long) 1e6, System.currentTimeMillis()), (long) 1e6, Types.STRING);
        //        DataStreamSource<String> dss1 = env.fromSource(s1,
        // WatermarkStrategy.noWatermarks(), "Generator Source 1");
        //        DataStreamSource<String> dss2 = env.fromSource(s2,
        // WatermarkStrategy.noWatermarks(), "Generator Source 2");

        ds1.coGroup(ds2)
                .where(value -> value)
                .equalTo(value -> value)
                //                .window(GlobalWindows.create())
                .window(EndOfStreamWindow.get())
                .apply(
                        new CoGroupFunction<Integer, Integer, Integer>() {
                            @Override
                            public void coGroup(
                                    Iterable<Integer> first,
                                    Iterable<Integer> second,
                                    Collector<Integer> out)
                                    throws Exception {
                                out.collect(1);
                            }
                            //                    @Override
                            //                    public Integer join(Integer first, Integer second)
                            // throws Exception {
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
                        })
                .addSink(new CountingAndDiscardingSink<>());

        long startTime = System.currentTimeMillis();
        JobExecutionResult executionResult = env.execute("");

        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
        System.out.println("Throughput: " + dataNum / (endTime - startTime));
        long count = executionResult.getAccumulatorResult(CountingAndDiscardingSink.COUNTER_NAME);
        System.out.println(count);
    }

    /**
     * A stream sink that counts the number of all elements. The counting result is stored in an
     * {@link org.apache.flink.api.common.accumulators.Accumulator} specified by {@link
     * #COUNTER_NAME} and can be acquired by {@link
     * org.apache.flink.api.common.JobExecutionResult#getAccumulatorResult(String)}.
     *
     * @param <T> The type of elements received by the sink.
     */
    private static class CountingAndDiscardingSink<T> extends RichSinkFunction<T> {
        public static final String COUNTER_NAME = "numElements";

        private static final long serialVersionUID = 1L;

        private final LongCounter numElementsCounter = new LongCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator(COUNTER_NAME, numElementsCounter);
        }

        @Override
        public void invoke(T value, Context context) {
            numElementsCounter.add(1L);
        }
    }
}
