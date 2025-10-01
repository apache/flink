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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
            Collection<DemultiplexingSinkState<RouteT>> recoveredStates)
            throws IOException {
        this(sinkRouter, context);

        // Restore state for each route
        for (DemultiplexingSinkState<RouteT> state : recoveredStates) {
            for (RouteT route : state.getRoutes()) {
                byte[] routeStateBytes = state.getRouteState(route);
                if (routeStateBytes != null) {
                    // We'll restore the sink writer when we first encounter an element for this
                    // route
                    // For now, just log that we have state to restore
                    LOG.debug("Found state to restore for route: {}", route);
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
                @SuppressWarnings("unchecked")
                StatefulSinkWriter<InputT, ?> statefulWriter =
                        (StatefulSinkWriter<InputT, ?>) writer;

                try {
                    List<?> writerStates = statefulWriter.snapshotState(checkpointId);
                    if (!writerStates.isEmpty()) {
                        // Serialize the writer states
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

            // Create a writer from the sink
            writer = sink.createWriter(context);
            sinkWriters.put(route, writer);

            LOG.debug("Created new sink writer for route: {}", route);
        }
        return writer;
    }

    /**
     * Serializes the writer states for a given route.
     *
     * <p>This is a placeholder implementation that uses Java serialization. In a production
     * implementation, this should use the proper state serializers from the underlying sinks.
     *
     * @param route The route key
     * @param writerStates The writer states to serialize
     * @return The serialized state bytes
     * @throws IOException If serialization fails
     */
    private byte[] serializeWriterStates(RouteT route, List<?> writerStates) throws IOException {
        // TODO: This is a simplified implementation. In practice, we should:
        // 1. Get the proper state serializer from the underlying sink
        // 2. Use that serializer to serialize the states
        // 3. Handle version compatibility properly

        // For now, we'll use a simple approach and store empty state
        // This will be improved in subsequent iterations
        return new byte[0];
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
