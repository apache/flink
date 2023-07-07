package org.apache.flink.test.flush;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.flush.util.FlushAggregator;
import org.apache.flink.test.flush.util.FlushSource;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.test.util.TestBaseUtils.compareResultAsTuples;

/** Tests for flushing module. */
public class AllowedLatencyITCase extends AbstractTestBase {
    private static Map<Integer, Long> testMap;

    private static List<Tuple2<Integer, Long>> testResult;

    private static long dataNum = 30000000L;

    @Test
    public void testAllowedLatency() throws Exception {
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < dataNum / 1000; ++i) {
            expected.append(i).append(",1000\n");
        }

        testMap = new HashMap<>();
        testResult = new ArrayList<>();

        Configuration config = new Configuration();
        config.setBoolean(DeploymentOptions.ATTACHED, true);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.getConfig()
                .enableObjectReuse()
                .setAutoWatermarkInterval(0)
                .setAllowedLatency(5000)
                .disableGenericTypes();
        env.setParallelism(1)
                .setStateBackend(new EmbeddedRocksDBStateBackend())
                .setRestartStrategy(RestartStrategies.noRestart());
        env.getCheckpointConfig().setCheckpointInterval(3000);

        DataStream<Integer> ds1 = env.addSource(new FlushSource(dataNum));

        ds1.keyBy(value -> value)
                .transform(
                        "MyAggregator",
                        new TupleTypeInfo<>(
                                IntegerTypeInfo.INT_TYPE_INFO, IntegerTypeInfo.LONG_TYPE_INFO),
                        new FlushAggregator(value -> value))
                .addSink(
                        new SinkFunction<Tuple2<Integer, Long>>() {
                            @Override
                            public void invoke(Tuple2<Integer, Long> value) throws Exception {
                                Long v = testMap.get(value.f0);
                                if (v == null || v < value.f1) {
                                    testMap.put(value.f0, value.f1);
                                }
                            }
                        });
        env.execute("AllowedLatencyITCase");

        testMap.forEach(
                (k, v) -> {
                    testResult.add(Tuple2.of(k, v));
                });
        compareResultAsTuples(testResult, expected.toString());
    }
}
