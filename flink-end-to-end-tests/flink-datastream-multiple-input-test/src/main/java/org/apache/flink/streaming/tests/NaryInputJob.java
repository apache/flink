package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Testing Stream with multiple input. */
public class NaryInputJob {

    private static final Logger LOG = LoggerFactory.getLogger(NaryInputJob.class);

    private static class IdleSource extends RichSourceFunction<byte[]> {

        private transient int ct = 0;
        private transient boolean isRunning = true;
        private transient boolean isIdle = false;
        private transient long startTs;
        private final int limit;

        public IdleSource(int limit) {
            this.limit = limit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ct = 0;
            isRunning = true;
            isIdle = false;
            startTs = System.currentTimeMillis();
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            while (isRunning) {
                ct += 1;
                if (!isIdle && ct > 10) {
                    isIdle = true;
                    ctx.markAsTemporarilyIdle();
                } else if (!isIdle) {
                    ctx.collectWithTimestamp(new byte[10], System.currentTimeMillis());
                } else {
                    if (ct > limit) {
                        isRunning = false;
                    }
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class HighThroughPutSource extends RichSourceFunction<byte[]> {
        private transient int ct = 0;
        private transient boolean isRunning = true;
        private transient boolean isIdle = false;
        private transient long startTs;
        private final int limit;

        public HighThroughPutSource(int limit) {
            this.limit = limit;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ct = 0;
            isRunning = true;
            isIdle = false;
            startTs = System.currentTimeMillis();
        }

        @Override
        public void run(SourceContext<byte[]> ctx) throws Exception {
            while (isRunning) {
                ct += 1;
                byte[] payload = new byte[1024];
                Arrays.fill(payload, (byte) (ct % 255));
                ctx.collectWithTimestamp(payload, System.currentTimeMillis());
                if (ct > limit) {
                    isRunning = false;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class NaryInputOperatorFactory extends AbstractStreamOperatorFactory<byte[]> {

        private final int numOfInputs;

        private NaryInputOperatorFactory(int numOfInputs) {
            this.numOfInputs = numOfInputs;
        }

        @Override
        public <T extends StreamOperator<byte[]>> T createStreamOperator(
                StreamOperatorParameters<byte[]> parameters) {
            return (T) new NaryInputOperator(parameters, numOfInputs);
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return NaryInputOperator.class;
        }
    }

    private static class NaryInputOperator extends AbstractStreamOperatorV2<byte[]>
            implements MultipleInputStreamOperator<byte[]> {

        private List<Input> inputs;

        public NaryInputOperator(StreamOperatorParameters<byte[]> parameters, int numberOfInputs) {
            super(parameters, numberOfInputs);
            inputs = new ArrayList<>(numberOfInputs);
            for (int i = 0; i < numberOfInputs; i++) {
                inputs.add(
                        new AbstractInput<byte[], byte[]>(this, i + 1) {
                            @Override
                            public void processElement(StreamRecord<byte[]> element)
                                    throws Exception {
                                output.collect(element);
                            }
                        });
            }
        }

        @Override
        public List<Input> getInputs() {
            return inputs;
        }
    }

    private static void connectAndDiscard(
            StreamExecutionEnvironment env, List<DataStream<byte[]>> sources) {
        MultipleInputTransformation<byte[]> transform =
                new MultipleInputTransformation<byte[]>(
                        "custom operator",
                        new NaryInputOperatorFactory(sources.size()),
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                        1);

        sources.forEach(
                source -> {
                    transform.addInput(source.getTransformation());
                });

        env.addOperator(transform);
        new MultipleConnectedStreams(env).transform(transform).addSink(new DiscardingSink<>());
    }

    public static void main(String[] args) throws Exception {

        ParameterTool pt = ParameterTool.fromArgs(args);

        int numOfIdle = pt.getInt("num-of-idle");
        int numOfTotal = pt.getInt("num-of-total");
        int limit = pt.getInt("limit");

        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();

        environment.getConfig().setAutoWatermarkInterval(200);
        environment.getConfig().enableObjectReuse();
        environment.getConfig().enableClosureCleaner();

        List<DataStream<byte[]>> sources =
                IntStream.range(0, numOfTotal)
                        .mapToObj(
                                i -> {
                                    if (i < numOfIdle) {
                                        // idle source
                                        return environment.addSource(
                                                new IdleSource(limit), "idle-source-" + i);
                                    } else {
                                        return environment.addSource(
                                                new HighThroughPutSource(limit),
                                                "high-throughput-source-" + (i - numOfIdle));
                                    }
                                })
                        .collect(Collectors.toList());

        connectAndDiscard(environment, sources);

        LOG.info("Starting nary-input-test");

        environment.execute("nary-input-test");
    }
}
