package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** Testing Stream with multiple input. */
public class OneHighThroughputOneBroadcastJob {

    private static final Logger LOG =
            LoggerFactory.getLogger(OneHighThroughputOneBroadcastJob.class);

    public static void main(String[] args) throws Exception {

        ParameterTool pt = ParameterTool.fromArgs(args);

        String durationStr = pt.get("run-for", "1M");

        long durationMillis = Duration.parse(durationStr).toMillis();

        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(200);
        environment.getConfig().enableObjectReuse();
        environment.getConfig().enableClosureCleaner();

        DataStream<byte[]> source1 =
                environment.addSource(
                        new RichSourceFunction<byte[]>() {
                            private transient int ct = 0;
                            private transient boolean isRunning = true;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                ct = 0;
                                isRunning = true;
                            }

                            @Override
                            public void run(SourceContext<byte[]> ctx) throws Exception {
                                long startTs = System.currentTimeMillis();
                                while (isRunning) {
                                    ct += 1;
                                    byte[] payload = new byte[1024];
                                    Arrays.fill(payload, (byte) (ct % 255));
                                    ctx.collectWithTimestamp(payload, System.currentTimeMillis());
                                    if (System.currentTimeMillis() - startTs > durationMillis) {
                                        isRunning = false;
                                    }
                                }
                            }

                            @Override
                            public void cancel() {
                                isRunning = false;
                            }
                        });

        MapStateDescriptor<String, List<Tuple2<Integer, Integer>>> desc =
                new MapStateDescriptor<>(
                        "broadcast-state",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new ListTypeInfo<>(
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})));

        BroadcastStream<Tuple2<Integer, Integer>> source2 =
                environment
                        .addSource(
                                new RichSourceFunction<Tuple2<Integer, Integer>>() {
                                    private transient int ct = 0;
                                    private transient boolean isRunning = true;
                                    private transient boolean isIdle = false;
                                    private transient long startTs;

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        super.open(parameters);
                                        ct = 0;
                                        isRunning = true;
                                        isIdle = false;
                                        startTs = System.currentTimeMillis();
                                    }

                                    @Override
                                    public void run(SourceContext<Tuple2<Integer, Integer>> ctx)
                                            throws Exception {
                                        while (isRunning) {
                                            ct += 1;
                                            if (!isIdle && ct > 10) {
                                                isIdle = true;
                                                ctx.markAsTemporarilyIdle();
                                            } else if (!isIdle) {
                                                ctx.collectWithTimestamp(
                                                        Tuple2.of(2, ct),
                                                        System.currentTimeMillis());
                                                //
                                                // try {
                                                //
                                                //  Thread.sleep(10);
                                                //                                                }
                                                // catch (InterruptedException e) {
                                                //
                                                //  // ignored
                                                //                                                }
                                            } else {
                                                if (System.currentTimeMillis() - startTs
                                                        > durationMillis) {
                                                    isRunning = false;
                                                }
                                                try {
                                                    Thread.sleep(100);
                                                } catch (InterruptedException e) {
                                                    // ignored
                                                }
                                            }
                                        }
                                    }

                                    @Override
                                    public void cancel() {
                                        isRunning = false;
                                    }
                                })
                        .broadcast(desc);

        DataStream<byte[]> stream1 =
                source1.connect(source2)
                        .process(
                                new BroadcastProcessFunction<
                                        byte[], Tuple2<Integer, Integer>, byte[]>() {

                                    @Override
                                    public void processElement(
                                            byte[] value,
                                            ReadOnlyContext ctx,
                                            Collector<byte[]> out)
                                            throws Exception {
                                        out.collect(value);
                                    }

                                    @Override
                                    public void processBroadcastElement(
                                            Tuple2<Integer, Integer> value,
                                            Context ctx,
                                            Collector<byte[]> out)
                                            throws Exception {}
                                });

        stream1.addSink(new DiscardingSink<>());

        LOG.info("Starting one-high-throughput-one-broadcast-test");

        environment.execute("one-high-throughput-one-broadcast-test");
    }
}
