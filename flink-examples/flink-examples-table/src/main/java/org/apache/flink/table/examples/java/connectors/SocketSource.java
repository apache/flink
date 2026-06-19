/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.examples.java.connectors.SocketSource.DummyCheckpoint;
import org.apache.flink.table.examples.java.connectors.SocketSource.DummySplit;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The {@link SocketSource} opens a socket and consumes bytes.
 *
 * <p>It splits records by the given byte delimiter (`\n` by default) and delegates the decoding to
 * a pluggable {@link DeserializationSchema}.
 *
 * <p>Note: This is only an example and should not be used in production. The source is not
 * fault-tolerant and can only work with a parallelism of 1.
 */
public final class SocketSource
        implements Source<RowData, DummySplit, DummyCheckpoint>, ResultTypeQueryable<RowData> {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;
    private final DeserializationSchema<RowData> deserializer;

    public SocketSource(
            String hostname,
            int port,
            byte byteDelimiter,
            DeserializationSchema<RowData> deserializer) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
        this.deserializer = deserializer;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return deserializer.getProducedType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> createEnumerator(
            SplitEnumeratorContext<DummySplit> enumContext) throws Exception {
        // The socket itself implicitly represents the only split and the enumerator is not
        // utilized.
        return null;
    }

    @Override
    public SplitEnumerator<DummySplit, DummyCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<DummySplit> enumContext, DummyCheckpoint checkpoint)
            throws Exception {
        // This source is not fault-tolerant.
        return null;
    }

    @Override
    public SimpleVersionedSerializer<DummySplit> getSplitSerializer() {
        return new NoOpDummySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DummyCheckpoint> getEnumeratorCheckpointSerializer() {
        // This source is not fault-tolerant.
        return null;
    }

    @Override
    public SourceReader<RowData, DummySplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        Preconditions.checkState(
                readerContext.currentParallelism() == 1,
                "SocketSource can only work with a parallelism of 1.");
        deserializer.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });
        return new SocketReader();
    }

    /**
     * Placeholder because the socket itself implicitly represents the only split and does not
     * require an actual split object.
     */
    public static class DummySplit implements SourceSplit {
        @Override
        public String splitId() {
            return "dummy";
        }
    }

    /**
     * Placeholder because the SocketSource does not support fault-tolerance and thus does not
     * require actual checkpointing.
     */
    public static class DummyCheckpoint {}

    private class SocketReader implements SourceReader<RowData, DummySplit> {

        private Socket socket;
        private ByteArrayOutputStream buffer;
        private InputStream stream;
        int b;

        @Override
        public void start() {
            while (socket == null) {
                try {
                    socket = new Socket();
                    socket.connect(new InetSocketAddress(hostname, port), 0);
                    buffer = new ByteArrayOutputStream();
                    stream = socket.getInputStream();
                } catch (Throwable t) {
                    socket = null;
                    try {
                        System.err.printf(
                                "Cannot connect to %s:%d. Retrying in 5 seconds...%n",
                                hostname, port);
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            while ((b = stream.read()) >= 0) {
                // buffer until delimiter
                if (b != byteDelimiter) {
                    buffer.write(b);
                }
                // decode and emit record
                else {
                    try {
                        output.collect(deserializer.deserialize(buffer.toByteArray()));
                    } catch (Exception e) {
                        System.err.printf(
                                "Malformed data row: %s. A valid sample: INSERT|Alice|12%n",
                                buffer.toString());
                    }
                    buffer.reset();
                    return InputStatus.MORE_AVAILABLE;
                }
            }
            return InputStatus.END_OF_INPUT;
        }

        @Override
        public List<DummySplit> snapshotState(long checkpointId) {
            // This source is not fault-tolerant.
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            // Not used. The socket is read in a blocking manner until it is closed.
            return null;
        }

        @Override
        public void addSplits(List<DummySplit> splits) {
            // Ignored. The socket itself implicitly represents the only split.
        }

        @Override
        public void notifyNoMoreSplits() {
            // Ignored. The socket itself implicitly represents the only split.
        }

        @Override
        public void close() throws Exception {
            try {
                buffer.close();
            } catch (Throwable t) {
                // ignore
            }

            try {
                stream.close();
            } catch (Throwable t) {
                // ignore
            }

            try {
                socket.close();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    /**
     * Not used - only required to avoid NullPointerException. The split is not transferred from the
     * enumerator, it is implicitly represented by the socket.
     */
    private static class NoOpDummySplitSerializer implements SimpleVersionedSerializer<DummySplit> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(DummySplit split) throws IOException {
            return new byte[0];
        }

        @Override
        public DummySplit deserialize(int version, byte[] serialized) throws IOException {
            return new DummySplit();
        }
    }
}
