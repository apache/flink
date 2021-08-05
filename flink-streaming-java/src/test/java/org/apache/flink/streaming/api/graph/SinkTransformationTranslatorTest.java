/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.SinkOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link org.apache.flink.streaming.api.transformations.SinkTransformation}. */
@RunWith(Parameterized.class)
public class SinkTransformationTranslatorTest extends TestLogger {

    @Parameterized.Parameters(name = "Execution Mode: {0}")
    public static Collection<Object> data() {
        return Arrays.asList(RuntimeExecutionMode.STREAMING, RuntimeExecutionMode.BATCH);
    }

    @Parameterized.Parameter() public RuntimeExecutionMode runtimeExecutionMode;

    static final String NAME = "FileSink";
    static final String SLOT_SHARE_GROUP = "FileGroup";
    static final String UID = "FileUid";
    static final int PARALLELISM = 2;

    @Test
    public void generateWriterTopology() {
        final StreamGraph streamGraph =
                buildGraph(TestSink.newBuilder().build(), runtimeExecutionMode);

        final StreamNode sourceNode = findNodeName(streamGraph, node -> node.contains("Source"));
        final StreamNode writerNode = findWriter(streamGraph);

        assertThat(streamGraph.getStreamNodes().size(), equalTo(2));

        validateTopology(
                sourceNode,
                IntSerializer.class,
                writerNode,
                SinkOperatorFactory.class,
                PARALLELISM,
                -1);
    }

    @Test
    public void generateWriterCommitterTopology() {

        final StreamGraph streamGraph =
                buildGraph(
                        TestSink.newBuilder().setDefaultCommitter().build(), runtimeExecutionMode);

        final StreamNode writerNode = findWriter(streamGraph);

        if (runtimeExecutionMode == RuntimeExecutionMode.STREAMING) {
            // in streaming writer and committer are merged into one operator
            assertThat(writerNode.getOutEdges(), equalTo(Collections.emptyList()));
            return;
        }

        final StreamNode committerNode =
                findNodeName(streamGraph, name -> name.contains("Committer"));

        assertThat(streamGraph.getStreamNodes().size(), equalTo(3));

        validateTopology(
                writerNode,
                BytePrimitiveArraySerializer.class,
                committerNode,
                CommitterOperatorFactory.class,
                runtimeExecutionMode == RuntimeExecutionMode.STREAMING ? PARALLELISM : 1,
                runtimeExecutionMode == RuntimeExecutionMode.STREAMING ? -1 : 1);
    }

    @Test
    public void generateWriterCommitterGlobalCommitterTopology() {

        final StreamGraph streamGraph =
                buildGraph(
                        TestSink.newBuilder()
                                .setDefaultCommitter()
                                .setDefaultGlobalCommitter()
                                .build(),
                        runtimeExecutionMode);

        final StreamNode writerNode = findWriter(streamGraph);
        final StreamNode committerNode = findCommitter(streamGraph);

        validateTopology(
                writerNode,
                BytePrimitiveArraySerializer.class,
                committerNode,
                CommitterOperatorFactory.class,
                1,
                1);
    }

    @Test
    public void generateWriterGlobalCommitterTopology() {
        final StreamGraph streamGraph =
                buildGraph(
                        TestSink.newBuilder()
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .setDefaultGlobalCommitter()
                                .build(),
                        runtimeExecutionMode);

        final StreamNode writerNode = findWriter(streamGraph);
        final StreamNode globalCommitterNode = findCommitter(streamGraph);

        validateTopology(
                writerNode,
                BytePrimitiveArraySerializer.class,
                globalCommitterNode,
                CommitterOperatorFactory.class,
                1,
                1);
    }

    private StreamNode findWriter(StreamGraph streamGraph) {
        return findNodeName(
                streamGraph, name -> name.contains(NAME) && !name.contains("Committer"));
    }

    private StreamNode findCommitter(StreamGraph streamGraph) {
        return findNodeName(streamGraph, name -> name.contains("Committer"));
    }

    @Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutSettingUid() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
        env.configure(config, getClass().getClassLoader());
        // disable auto generating uid
        env.getConfig().disableAutoGeneratedUIDs();
        env.fromElements(1, 2).sinkTo(TestSink.newBuilder().build());
        env.getStreamGraph();
    }

    @Test
    public void disableOperatorChain() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        final DataStreamSink<Integer> dataStreamSink =
                src.sinkTo(
                                TestSink.newBuilder()
                                        .setDefaultCommitter()
                                        .setDefaultGlobalCommitter()
                                        .build())
                        .name(NAME);
        dataStreamSink.disableChaining();

        final StreamGraph streamGraph = env.getStreamGraph();
        final StreamNode writer = findWriter(streamGraph);
        final StreamNode globalCommitter = findCommitter(streamGraph);

        assertThat(writer.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.NEVER));
        assertThat(
                globalCommitter.getOperatorFactory().getChainingStrategy(),
                is(ChainingStrategy.ALWAYS));
    }

    private void validateTopology(
            StreamNode src,
            Class<?> srcOutTypeInfo,
            StreamNode dest,
            Class<? extends StreamOperatorFactory> operatorFactoryClass,
            int expectedParallelism,
            int expectedMaxParallelism) {

        // verify src node
        final StreamEdge srcOutEdge = src.getOutEdges().get(0);
        assertThat(srcOutEdge.getTargetId(), equalTo(dest.getId()));
        assertThat(src.getTypeSerializerOut(), instanceOf(srcOutTypeInfo));

        // verify dest node input
        final StreamEdge destInputEdge = dest.getInEdges().get(0);
        assertThat(destInputEdge.getSourceId(), equalTo(src.getId()));
        assertThat(dest.getTypeSerializersIn()[0], instanceOf(srcOutTypeInfo));

        // make sure 2 sink operators have different names/uid
        assertThat(dest.getOperatorName(), not(equalTo(src.getOperatorName())));
        assertThat(dest.getTransformationUID(), not(equalTo(src.getTransformationUID())));

        assertThat(dest.getOperatorFactory(), instanceOf(operatorFactoryClass));
        assertThat(dest.getParallelism(), equalTo(expectedParallelism));
        assertThat(dest.getMaxParallelism(), equalTo(expectedMaxParallelism));
        assertThat(dest.getOperatorFactory().getChainingStrategy(), is(ChainingStrategy.ALWAYS));
        assertThat(dest.getSlotSharingGroup(), equalTo(SLOT_SHARE_GROUP));

        // verify dest node output
        assertThat(dest.getOutEdges().size(), equalTo(0));
    }

    private StreamGraph buildGraph(TestSink sink, RuntimeExecutionMode runtimeExecutionMode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final Configuration config = new Configuration();
        config.set(ExecutionOptions.RUNTIME_MODE, runtimeExecutionMode);
        env.configure(config, getClass().getClassLoader());
        final DataStreamSource<Integer> src = env.fromElements(1, 2);
        final DataStreamSink<Integer> dataStreamSink = src.rebalance().sinkTo(sink);
        setSinkProperty(dataStreamSink);
        return env.getStreamGraph();
    }

    private void setSinkProperty(DataStreamSink<Integer> dataStreamSink) {
        dataStreamSink.name(NAME);
        dataStreamSink.uid(UID);
        dataStreamSink.setParallelism(SinkTransformationTranslatorTest.PARALLELISM);
        dataStreamSink.slotSharingGroup(SLOT_SHARE_GROUP);
    }

    private StreamNode findNodeName(StreamGraph streamGraph, Predicate<String> predicate) {
        return streamGraph.getStreamNodes().stream()
                .filter(node -> predicate.test(node.getOperatorName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Can not find the node"));
    }
}
