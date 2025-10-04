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

package org.apache.flink.connector.base.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A sink writer that routes elements to different underlying sink writers based on a routing
 * function.
 *
 * <p>This writer maintains a cache of sink writers, creating new ones on-demand when previously
 * unseen routes are encountered. Each underlying sink writer is managed independently, including
 * their lifecycle, state management, and error handling.
 *
 * <p>The writer supports state management by collecting state from all underlying sink writers
 * during checkpointing and restoring them appropriately during recovery.
 *
 * @param <InputT> The type of input elements
 * @param <RouteT> The type of route keys used for sink selection
 */
@PublicEvolving
public class DemultiplexingSinkWriter<InputT, RouteT>
        implements StatefulSinkWriter<InputT, DemultiplexingSinkState<RouteT>> {

    private static final Logger LOG = LoggerFactory.getLogger(DemultiplexingSinkWriter.class);

    /** The router that determines how elements are routed to sinks. */
    private final SinkRouter<InputT, RouteT> sinkRouter;

    /** The writer initialization context. */
    private final WriterInitContext context;

    /** Cache of sink writers by route key. */
    private final Map<RouteT, SinkWriter<InputT>> sinkWriters;

    /** Cache of sink instances by route key for creating writers. */
    private final Map<RouteT, Sink<InputT>> sinks;

    /** Cache of recovered states by route key for lazy restoration. */
    private final Map<RouteT, List<Object>> recoveredStates;

    /**
     * Creates a new demultiplexing sink writer.
     *
     * @param sinkRouter The router that determines how elements are routed to different sinks
     * @param context The writer initialization context
     */
    public DemultiplexingSinkWriter(
            SinkRouter<InputT, RouteT> sinkRouter, WriterInitContext context) {
        this.sinkRouter = Preconditions.checkNotNull(sinkRouter);
        this.context = Preconditions.checkNotNull(context);
        this.sinkWriters = new HashMap<>();
        this.sinks = new HashMap<>();
        this.recoveredStates = new HashMap<>();
    }

    /**
     * Creates a new demultiplexing sink writer and restores state from a previous checkpoint.
     *
     * @param sinkRouter The router that determines how elements are routed to different sinks
     * @param context The writer initialization context
     * @param recoveredStates The recovered states from previous checkpoints
     */
    public DemultiplexingSinkWriter(
            SinkRouter<InputT, RouteT> sinkRouter,
            WriterInitContext context,
            Collection<DemultiplexingSinkState<RouteT>> recoveredStates) {
        this(sinkRouter, context);

        // Process recovered states and prepare them for lazy restoration
        for (DemultiplexingSinkState<RouteT> state : recoveredStates) {
            for (RouteT route : state.getRoutes()) {
                byte[] routeStateBytes = state.getRouteState(route);
                if (routeStateBytes != null && routeStateBytes.length > 0) {
                    try {
                        // Deserialize the writer states for this route
                        List<Object> writerStates = deserializeWriterStates(route, routeStateBytes);
                        this.recoveredStates.put(route, writerStates);
                        LOG.debug(
                                "Prepared state restoration for route: {} with {} states",
                                route,
                                writerStates.size());
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to deserialize state for route: {}, will start with empty state",
                                route,
                                e);
                    }
                }
            }
        }
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        // Determine the route for this element
        final RouteT route = sinkRouter.getRoute(element);

        // Get or create the sink writer for this route
        SinkWriter<InputT> writer = getOrCreateSinkWriter(route, element);

        // Delegate to the appropriate sink writer
        writer.write(element, context);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // Flush all active sink writers
        IOException lastException = null;
        for (Map.Entry<RouteT, SinkWriter<InputT>> entry : sinkWriters.entrySet()) {
            try {
                entry.getValue().flush(endOfInput);
            } catch (IOException e) {
                LOG.warn("Failed to flush sink writer for route: {}", entry.getKey(), e);
                lastException = e;
            }
        }

        // Re-throw the last exception if any occurred
        if (lastException != null) {
            throw lastException;
        }
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        // Propagate watermark to all active sink writers
        IOException lastException = null;
        for (Map.Entry<RouteT, SinkWriter<InputT>> entry : sinkWriters.entrySet()) {
            try {
                entry.getValue().writeWatermark(watermark);
            } catch (IOException e) {
                LOG.warn(
                        "Failed to write watermark to sink writer for route: {}",
                        entry.getKey(),
                        e);
                lastException = e;
            }
        }

        // Re-throw the last exception if any occurred
        if (lastException != null) {
            throw lastException;
        }
    }

    @Override
    public List<DemultiplexingSinkState<RouteT>> snapshotState(long checkpointId)
            throws IOException {
        final Map<RouteT, byte[]> routeStates = new HashMap<>();

        // Collect state from all active sink writers
        for (Map.Entry<RouteT, SinkWriter<InputT>> entry : sinkWriters.entrySet()) {
            final RouteT route = entry.getKey();
            final SinkWriter<InputT> writer = entry.getValue();

            // Only collect state from stateful sink writers
            if (writer instanceof StatefulSinkWriter) {
                StatefulSinkWriter<InputT, ?> statefulWriter =
                        (StatefulSinkWriter<InputT, ?>) writer;

                try {
                    List<?> writerStates = statefulWriter.snapshotState(checkpointId);
                    if (!writerStates.isEmpty()) {
                        // Serialize the writer states and store them
                        byte[] serializedState = serializeWriterStates(route, writerStates);
                        routeStates.put(route, serializedState);
                    }
                } catch (Exception e) {
                    LOG.warn("Failed to snapshot state for route: {}", route, e);
                    throw new IOException("Failed to snapshot state for route: " + route, e);
                }
            }
        }

        // Return a single state object containing all route states
        final List<DemultiplexingSinkState<RouteT>> states = new ArrayList<>();
        if (!routeStates.isEmpty()) {
            states.add(new DemultiplexingSinkState<>(routeStates));
        }

        return states;
    }

    @Override
    public void close() throws Exception {
        // Close all active sink writers
        Exception lastException = null;
        for (Map.Entry<RouteT, SinkWriter<InputT>> entry : sinkWriters.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.warn("Failed to close sink writer for route: {}", entry.getKey(), e);
                lastException = e;
            }
        }

        // Clear the caches
        sinkWriters.clear();
        sinks.clear();

        // Re-throw the last exception if any occurred
        if (lastException != null) {
            throw lastException;
        }
    }

    /**
     * Gets or creates a sink writer for the given route.
     *
     * @param route The route key
     * @param element The element that triggered this route (used for sink creation)
     * @return The sink writer for the route
     * @throws IOException If sink or writer creation fails
     */
    private SinkWriter<InputT> getOrCreateSinkWriter(RouteT route, InputT element)
            throws IOException {
        SinkWriter<InputT> writer = sinkWriters.get(route);
        if (writer == null) {
            // Create a new sink for this route
            Sink<InputT> sink = sinkRouter.createSink(route, element);
            sinks.put(route, sink);

            // Check if we have recovered state for this route
            List<Object> routeRecoveredStates = recoveredStates.remove(route);

            if (routeRecoveredStates != null && !routeRecoveredStates.isEmpty()) {
                // Restore writer with state if the sink supports it
                if (sink instanceof SupportsWriterState) {
                    @SuppressWarnings("unchecked")
                    SupportsWriterState<InputT, Object> statefulSink =
                            (SupportsWriterState<InputT, Object>) sink;

                    try {
                        // Check if we need to deserialize raw bytes using the sink's serializer
                        List<Object> processedStates =
                                processRecoveredStates(statefulSink, routeRecoveredStates);

                        writer = statefulSink.restoreWriter(context, processedStates);
                        LOG.debug(
                                "Restored sink writer for route: {} with {} states",
                                route,
                                processedStates.size());
                    } catch (Exception e) {
                        LOG.warn(
                                "Failed to restore writer state for route: {}, creating new writer",
                                route,
                                e);
                        writer = sink.createWriter(context);
                    }
                } else {
                    // Sink doesn't support state, just create a new writer
                    writer = sink.createWriter(context);
                    LOG.debug("Sink for route {} doesn't support state, created new writer", route);
                }
            } else {
                // No recovered state, create a new writer
                writer = sink.createWriter(context);
                LOG.debug("Created new sink writer for route: {} (no recovered state)", route);
            }

            sinkWriters.put(route, writer);
        }
        return writer;
    }

    /**
     * Processes recovered states, deserializing raw bytes if necessary.
     *
     * @param statefulSink The stateful sink that can provide a state serializer
     * @param recoveredStates The recovered states (may contain raw bytes)
     * @return Processed states ready for restoration
     */
    private List<Object> processRecoveredStates(
            SupportsWriterState<InputT, Object> statefulSink, List<Object> recoveredStates) {

        List<Object> processedStates = new ArrayList<>();
        SimpleVersionedSerializer<Object> serializer = statefulSink.getWriterStateSerializer();

        for (Object state : recoveredStates) {
            if (state instanceof byte[]) {
                // This is raw bytes that need to be deserialized using the sink's serializer
                byte[] rawBytes = (byte[]) state;
                try {
                    List<Object> deserializedStates =
                            deserializeWithSinkSerializer(serializer, rawBytes);
                    processedStates.addAll(deserializedStates);
                    LOG.debug(
                            "Successfully deserialized {} states from raw bytes using sink serializer",
                            deserializedStates.size());
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to deserialize raw bytes using sink serializer, skipping state",
                            e);
                }
            } else {
                // This is already a deserialized state object
                processedStates.add(state);
            }
        }

        return processedStates;
    }

    /**
     * Deserializes states using the sink's state serializer.
     *
     * @param serializer The sink's state serializer
     * @param rawBytes The raw serialized bytes
     * @return List of deserialized state objects
     * @throws IOException If deserialization fails
     */
    private List<Object> deserializeWithSinkSerializer(
            SimpleVersionedSerializer<Object> serializer, byte[] rawBytes) throws IOException {

        List<Object> states = new ArrayList<>();

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
                final java.io.DataInputStream dis = new java.io.DataInputStream(bais)) {

            // Read the number of states
            int numStates = dis.readInt();

            // Read each state
            for (int i = 0; i < numStates; i++) {
                int stateLength = dis.readInt();
                byte[] stateBytes = new byte[stateLength];
                dis.readFully(stateBytes);

                Object state = serializer.deserialize(serializer.getVersion(), stateBytes);
                states.add(state);
            }
        }

        return states;
    }

    /**
     * Serializes the writer states for a given route.
     *
     * <p>This implementation attempts to use the proper state serializer from the underlying sink
     * if available, otherwise falls back to Java serialization.
     *
     * @param route The route key
     * @param writerStates The writer states to serialize
     * @return The serialized state bytes
     * @throws IOException If serialization fails
     */
    private byte[] serializeWriterStates(RouteT route, List<?> writerStates) throws IOException {
        if (writerStates == null || writerStates.isEmpty()) {
            return new byte[0];
        }

        Sink<InputT> sink = sinks.get(route);
        if (sink instanceof SupportsWriterState) {
            try {
                @SuppressWarnings("unchecked")
                SupportsWriterState<InputT, Object> statefulSink =
                        (SupportsWriterState<InputT, Object>) sink;
                SimpleVersionedSerializer<Object> serializer =
                        statefulSink.getWriterStateSerializer();

                // Serialize each state and combine them
                try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        final DataOutputStream dos = new DataOutputStream(baos)) {

                    // Write the number of states
                    dos.writeInt(writerStates.size());

                    // Write each state
                    for (Object state : writerStates) {
                        byte[] stateBytes = serializer.serialize(state);
                        dos.writeInt(stateBytes.length);
                        dos.write(stateBytes);
                    }

                    dos.flush();
                    return baos.toByteArray();
                }
            } catch (Exception e) {
                LOG.warn(
                        "Failed to serialize state using sink serializer for route: {}, "
                                + "falling back to Java serialization",
                        route,
                        e);
            }
        }

        // Fallback to Java serialization
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(writerStates);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new IOException("Failed to serialize writer states for route: " + route, e);
        }
    }

    /**
     * Deserializes the writer states for a given route.
     *
     * <p>This method attempts to deserialize states using Java serialization as a fallback. The
     * proper sink-specific deserialization will happen later when the sink is created and we can
     * access its state serializer.
     *
     * @param route The route key
     * @param stateBytes The serialized state bytes
     * @return The deserialized writer states (or raw bytes to be deserialized later)
     */
    private List<Object> deserializeWriterStates(RouteT route, byte[] stateBytes) {
        if (stateBytes == null || stateBytes.length == 0) {
            return new ArrayList<>();
        }

        // Try Java deserialization first (this handles the fallback case)
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(stateBytes);
                final ObjectInputStream ois = new ObjectInputStream(bais)) {
            @SuppressWarnings("unchecked")
            List<Object> states = (List<Object>) ois.readObject();
            LOG.debug(
                    "Successfully deserialized {} states for route {} using Java serialization",
                    states.size(),
                    route);
            return states;
        } catch (Exception e) {
            LOG.debug(
                    "Java deserialization failed for route {}, will store raw bytes for later processing",
                    route);
            // If Java deserialization fails, store the raw bytes
            // They will be processed when we have access to the sink's serializer
            List<Object> rawStates = new ArrayList<>();
            rawStates.add(stateBytes); // Store as raw bytes
            return rawStates;
        }
    }

    /**
     * Gets the number of currently active sink writers.
     *
     * @return The number of active sink writers
     */
    public int getActiveSinkWriterCount() {
        return sinkWriters.size();
    }

    /**
     * Gets the routes for all currently active sink writers.
     *
     * @return A collection of route keys for active sink writers
     */
    public Collection<RouteT> getActiveRoutes() {
        return new ArrayList<>(sinkWriters.keySet());
    }
}
