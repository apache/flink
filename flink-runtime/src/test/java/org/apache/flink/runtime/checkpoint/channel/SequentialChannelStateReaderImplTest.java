/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.flink.shaded.guava31.com.google.common.io.Closer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link SequentialChannelStateReaderImpl} Test. */
@ExtendWith(ParameterizedTestExtension.class)
public class SequentialChannelStateReaderImplTest {

    @Parameters(
            name =
                    "{0}: stateParLevel={1}, statePartsPerChannel={2}, stateBytesPerPart={3},  parLevel={4}, bufferSize={5}")
    public static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {"NoStateAndNoChannels", 0, 0, 0, 0, 0},
                new Object[] {"NoState", 0, 10, 10, 10, 10},
                new Object[] {"ReadPermutedStateWithEqualBuffer", 10, 10, 10, 10, 10},
                new Object[] {"ReadPermutedStateWithReducedBuffer", 10, 10, 10, 20, 10},
                new Object[] {"ReadPermutedStateWithIncreasedBuffer", 10, 10, 10, 10, 20});
    }

    @Parameter public String desc;

    @Parameter(value = 1)
    public int stateParLevel;

    @Parameter(value = 2)
    public int statePartsPerChannel;

    @Parameter(value = 3)
    public int stateBytesPerPart;

    @Parameter(value = 4)
    public int parLevel;

    @Parameter(value = 5)
    public int bufferSize;

    private ChannelStateSerializer serializer;
    private Random random;
    private int buffersPerChannel;

    @BeforeEach
    void before() {
        serializer = new ChannelStateSerializerImpl();
        random = new Random();
        // will read without waiting for consumption
        buffersPerChannel =
                Math.max(
                        1,
                        statePartsPerChannel
                                * (bufferSize >= stateBytesPerPart
                                        ? 1
                                        : stateBytesPerPart / bufferSize));
    }

    @TestTemplate
    void testReadPermutedState() throws Exception {
        Map<InputChannelInfo, List<byte[]>> inputChannelsData =
                generateState(InputChannelInfo::new);
        Map<ResultSubpartitionInfo, List<byte[]>> resultPartitionsData =
                generateState(ResultSubpartitionInfo::new);

        SequentialChannelStateReader reader =
                new SequentialChannelStateReaderImpl(
                        buildSnapshot(writePermuted(inputChannelsData, resultPartitionsData)));

        withResultPartitions(
                resultPartitions -> {
                    reader.readOutputData(resultPartitions, false);
                    assertBuffersEquals(resultPartitionsData, collectBuffers(resultPartitions));
                });

        withInputGates(
                gates -> {
                    reader.readInputData(gates);
                    assertBuffersEquals(inputChannelsData, collectBuffers(gates));
                    assertConsumed(gates);
                });
    }

    private Map<ResultSubpartitionInfo, List<Buffer>> collectBuffers(
            BufferWritingResultPartition[] resultPartitions) throws IOException {
        Map<ResultSubpartitionInfo, List<Buffer>> actual = new HashMap<>();
        for (BufferWritingResultPartition resultPartition : resultPartitions) {
            for (int i = 0; i < resultPartition.getNumberOfSubpartitions(); i++) {
                ResultSubpartitionInfo info =
                        resultPartition.getAllPartitions()[i].getSubpartitionInfo();
                ResultSubpartitionView view =
                        resultPartition.createSubpartitionView(
                                info.getSubPartitionIdx(), new NoOpBufferAvailablityListener());
                for (BufferAndBacklog buffer = view.getNextBuffer();
                        buffer != null;
                        buffer = view.getNextBuffer()) {
                    if (buffer.buffer().isBuffer()) {
                        actual.computeIfAbsent(info, unused -> new ArrayList<>())
                                .add(buffer.buffer());
                    }
                }
            }
        }
        return actual;
    }

    private Map<InputChannelInfo, List<Buffer>> collectBuffers(InputGate[] gates) throws Exception {
        Map<InputChannelInfo, List<Buffer>> actual = new HashMap<>();
        for (InputGate gate : gates) {
            for (Optional<BufferOrEvent> next = gate.pollNext();
                    next.isPresent();
                    next = gate.pollNext()) {
                if (next.get().isBuffer()) {
                    actual.computeIfAbsent(next.get().getChannelInfo(), unused -> new ArrayList<>())
                            .add(next.get().getBuffer());
                }
            }
        }
        return actual;
    }

    private void assertConsumed(InputGate[] gates)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (InputGate gate : gates) {
            assertThat(gate.getStateConsumedFuture()).isDone();
            gate.getStateConsumedFuture().get();
        }
    }

    private void withInputGates(ThrowingConsumer<InputGate[], Exception> action) throws Exception {
        SingleInputGate[] gates = new SingleInputGate[parLevel];
        final int segmentsToAllocate = parLevel + parLevel * parLevel * buffersPerChannel;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(segmentsToAllocate, bufferSize);
        try (Closer poolCloser = Closer.create()) {
            poolCloser.register(networkBufferPool::destroy);
            poolCloser.register(networkBufferPool::destroyAllBufferPools);

            try (Closer gateCloser = Closer.create()) {
                for (int i = 0; i < parLevel; i++) {
                    gates[i] =
                            new SingleInputGateBuilder()
                                    .setNumberOfChannels(parLevel)
                                    .setSingleInputGateIndex(i)
                                    .setBufferPoolFactory(
                                            networkBufferPool.createBufferPool(
                                                    1, buffersPerChannel))
                                    .setSegmentProvider(networkBufferPool)
                                    .setChannelFactory(
                                            (builder, gate) ->
                                                    builder.setNetworkBuffersPerChannel(
                                                                    buffersPerChannel)
                                                            .buildRemoteRecoveredChannel(gate))
                                    .build();
                    gates[i].setup();
                    gateCloser.register(gates[i]::close);
                }
                action.accept(gates);
            }
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                    .isEqualTo(segmentsToAllocate);
        }
    }

    private void withResultPartitions(
            ThrowingConsumer<BufferWritingResultPartition[], Exception> action) throws Exception {
        int segmentsToAllocate = parLevel * parLevel * buffersPerChannel;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(segmentsToAllocate, bufferSize);
        BufferWritingResultPartition[] resultPartitions =
                range(0, parLevel)
                        .mapToObj(
                                i ->
                                        new ResultPartitionBuilder()
                                                .setResultPartitionIndex(i)
                                                .setNumberOfSubpartitions(parLevel)
                                                .setNetworkBufferPool(networkBufferPool)
                                                .build())
                        .toArray(BufferWritingResultPartition[]::new);
        try {
            for (ResultPartition resultPartition : resultPartitions) {
                resultPartition.setup();
            }
            action.accept(resultPartitions);
        } finally {
            for (ResultPartition resultPartition : resultPartitions) {
                resultPartition.close();
            }
            try {
                assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                        .isEqualTo(segmentsToAllocate);
            } finally {
                networkBufferPool.destroyAllBufferPools();
                networkBufferPool.destroy();
            }
        }
    }

    private TaskStateSnapshot buildSnapshot(
            Tuple2<List<InputChannelStateHandle>, List<ResultSubpartitionStateHandle>> handles) {
        return new TaskStateSnapshot(
                Collections.singletonMap(
                        new OperatorID(),
                        OperatorSubtaskState.builder()
                                .setInputChannelState(new StateObjectCollection<>(handles.f0))
                                .setResultSubpartitionState(new StateObjectCollection<>(handles.f1))
                                .build()));
    }

    private <T> Map<T, List<byte[]>> generateState(
            BiFunction<Integer, Integer, T> descriptorCreator) {
        return range(0, stateParLevel)
                .boxed()
                .flatMap(
                        gateId ->
                                range(0, stateParLevel)
                                        .mapToObj(
                                                channelId ->
                                                        descriptorCreator.apply(gateId, channelId)))
                .collect(toMap(identity(), this::generateSingleChannelState));
    }

    private List<byte[]> generateSingleChannelState(Object handle) {
        return range(0, statePartsPerChannel)
                .mapToObj(unused -> randomStateBytes())
                .collect(toList());
    }

    private Tuple2<List<InputChannelStateHandle>, List<ResultSubpartitionStateHandle>>
            writePermuted(
                    Map<InputChannelInfo, List<byte[]>> inputChannels,
                    Map<ResultSubpartitionInfo, List<byte[]>> resultSubpartitions)
                    throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DataOutputStream dataStream = new DataOutputStream(out);
            serializer.writeHeader(dataStream);

            Map<InputChannelInfo, List<Long>> icOffsets = write(dataStream, permute(inputChannels));
            Map<ResultSubpartitionInfo, List<Long>> rsOffsets =
                    write(dataStream, permute(resultSubpartitions));

            ByteStreamStateHandle streamStateHandle =
                    new ByteStreamStateHandle("", out.toByteArray());
            return Tuple2.of(
                    icOffsets.entrySet().stream()
                            .map(
                                    e ->
                                            new InputChannelStateHandle(
                                                    e.getKey(), streamStateHandle, e.getValue()))
                            .collect(toList()),
                    rsOffsets.entrySet().stream()
                            .map(
                                    e ->
                                            new ResultSubpartitionStateHandle(
                                                    e.getKey(), streamStateHandle, e.getValue()))
                            .collect(toList()));
        }
    }

    private <T> List<Tuple2<byte[], T>> permute(Map<T, List<byte[]>> inputChannels) {
        List<Map.Entry<T, List<byte[]>>> entries = new ArrayList<>(inputChannels.entrySet());
        Collections.shuffle(
                entries); // permute across channels, but not across buffers of a single channel
        return entries.stream()
                .flatMap(e -> e.getValue().stream().map(b -> Tuple2.of(b, e.getKey())))
                .collect(toList());
    }

    private <T> Map<T, List<Long>> write(
            DataOutputStream dataStream, List<Tuple2<byte[], T>> partsPermuted) throws IOException {
        Map<T, List<Long>> offsets = new HashMap<>();
        for (Tuple2<byte[], T> t2 : partsPermuted) {
            offsets.computeIfAbsent(t2.f1, unused -> new ArrayList<>())
                    .add((long) dataStream.size());
            NetworkBuffer networkBuffer = null;
            try {
                final byte[] bytes = t2.f0;
                networkBuffer = wrap(bytes);
                serializer.writeData(dataStream, networkBuffer);
            } finally {
                if (networkBuffer != null) {
                    networkBuffer.recycleBuffer();
                }
            }
        }
        return offsets;
    }

    private NetworkBuffer wrap(byte[] bytes) {
        return new NetworkBuffer(
                MemorySegmentFactory.wrap(bytes),
                FreeingBufferRecycler.INSTANCE,
                Buffer.DataType.DATA_BUFFER,
                bytes.length);
    }

    private byte[] randomStateBytes() {
        final byte[] buf = new byte[stateBytesPerPart];
        random.nextBytes(buf);
        return buf;
    }

    private <T> void assertBuffersEquals(
            Map<T, List<byte[]>> expected, Map<T, List<Buffer>> actual) {
        try {
            assertThat(mapValues(actual, buffers -> concat(toBytes(buffers))))
                    .isEqualTo(mapValues(expected, this::concat));
        } finally {
            actual.values().stream().flatMap(List::stream).forEach(Buffer::recycleBuffer);
        }
    }

    private static <K, V1, V2> Map<K, V2> mapValues(Map<K, V1> map, Function<V1, V2> mapFn) {
        return map.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> mapFn.apply(e.getValue())));
    }

    private NetworkBuffer concat(List<byte[]> list) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            for (byte[] bytes : list) {
                outputStream.write(bytes);
            }
            return wrap(outputStream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<byte[]> toBytes(List<Buffer> buffers) {
        return buffers.stream()
                .map(
                        buffer -> {
                            byte[] buf = new byte[buffer.getSize()];
                            buffer.getNioBuffer(0, buffer.getSize()).get(buf, 0, buf.length);
                            return buf;
                        })
                .collect(toList());
    }
}
