package org.apache.flink.streaming.examples.allowlatency;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.allowlatency.utils.CountingAndDiscardingSink;
import org.apache.flink.streaming.examples.allowlatency.utils.MyAggregator;
import org.apache.flink.streaming.examples.allowlatency.utils.MyJoinSource;

import java.util.HashMap;
import java.util.Map;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the streams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the streams of pairs at a
 * configurable rate.
 */
public class FlushInterval {
    private static Map<Integer, Long> testMap;

    public static void main(String[] args) throws Exception {
        testMap = new HashMap<>();

        final Map<String, String> params = MultipleParameterTool.fromArgs(args).toMap();

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
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        Configuration conf = Configuration.fromMap(params);
        env.getConfig().configure(conf, Thread.currentThread().getContextClassLoader());

        long dataNum = params.get("dataNum") == null ? 100000000 : Long.parseLong(params.get("dataNum"));
        System.out.println("Number of records: " + dataNum);
        System.out.println("flush enabled: " + env.getConfig().getFLushEnabled());
        long pause = params.get("pause") == null ? dataNum / 5: Long.parseLong(params.get("pause"));

        DataStream<Integer> ds1 = env.addSource(new MyJoinSource(dataNum, pause, 0));
        ds1.keyBy(value -> value)
                .transform(
                        "MyAggregator",
                        new TupleTypeInfo<>(
                                IntegerTypeInfo.INT_TYPE_INFO, IntegerTypeInfo.LONG_TYPE_INFO),
                        new MyAggregator(value -> value))
                .addSink(new CountingAndDiscardingSink<>());
//                .addSink(
//                        new SinkFunction<Tuple2<Integer, Long>>() {
//                            @Override
//                            public void invoke(Tuple2<Integer, Long> value) throws Exception {
//                                Long v = testMap.get(value.f0);
//                                if (v == null || v < value.f1) {
//                                    testMap.put(value.f0, value.f1);
//                                }
//                            }
//                        });


        long startTime = System.currentTimeMillis();
        JobExecutionResult executionResult = env.execute("");
        long endTime = System.currentTimeMillis();
        System.out.println("Duration: " + (endTime - startTime));
        System.out.println("Throughput: " + (dataNum / (endTime - startTime)));
//        testMap.forEach(
//                (k, v) -> {
//                    System.out.println(k + ": " + v);
//                });
    }

}

