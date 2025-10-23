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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for {@link DemultiplexingSinkState}.
 *
 * <p>This serializer handles the serialization of the demultiplexing sink state, which contains a
 * mapping of route keys to their corresponding sink writer states. The route keys are serialized
 * using Java serialization, while the sink writer states are stored as raw byte arrays.
 *
 * <p>Serialization format:
 *
 * <ul>
 *   <li>Version (int)
 *   <li>Number of routes (int)
 *   <li>For each route:
 *       <ul>
 *         <li>Route key length (int)
 *         <li>Route key bytes (serialized using Java serialization)
 *         <li>State length (int)
 *         <li>State bytes
 *       </ul>
 * </ul>
 *
 * @param <RouteT> The type of route keys
 */
@PublicEvolving
public class DemultiplexingSinkStateSerializer<RouteT>
        implements SimpleVersionedSerializer<DemultiplexingSinkState<RouteT>> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(DemultiplexingSinkState<RouteT> state) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(256);

        // Write the number of routes
        final Map<RouteT, byte[]> routeStates = state.getRouteStates();
        out.writeInt(routeStates.size());

        // Write each route and its state
        for (Map.Entry<RouteT, byte[]> entry : routeStates.entrySet()) {
            // Serialize the route key using Java serialization
            final byte[] routeBytes = serializeRouteKey(entry.getKey());
            out.writeInt(routeBytes.length);
            out.write(routeBytes);

            // Write the state bytes
            final byte[] stateBytes = entry.getValue();
            out.writeInt(stateBytes.length);
            out.write(stateBytes);
        }

        return out.getCopyOfBuffer();
    }

    @Override
    public DemultiplexingSinkState<RouteT> deserialize(int version, byte[] serialized)
            throws IOException {
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported version: " + version + ". Supported version: " + VERSION);
        }

        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        // Read the number of routes
        final int numRoutes = in.readInt();
        final Map<RouteT, byte[]> routeStates = new HashMap<>(numRoutes);

        // Read each route and its state
        for (int i = 0; i < numRoutes; i++) {
            // Read the route key
            final int routeLength = in.readInt();
            final byte[] routeBytes = new byte[routeLength];
            in.readFully(routeBytes);
            final RouteT route = deserializeRouteKey(routeBytes);

            // Read the state bytes
            final int stateLength = in.readInt();
            final byte[] stateBytes = new byte[stateLength];
            in.readFully(stateBytes);

            // Store the route states
            routeStates.put(route, stateBytes);
        }

        return new DemultiplexingSinkState<>(routeStates);
    }

    /**
     * Serializes a route key using Java serialization.
     *
     * @param routeKey The route key to serialize
     * @return The serialized route key bytes
     * @throws IOException If serialization fails
     */
    private byte[] serializeRouteKey(RouteT routeKey) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(routeKey);
            oos.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new IOException("Failed to serialize route key: " + routeKey, e);
        }
    }

    /**
     * Deserializes a route key using Java serialization.
     *
     * @param routeBytes The serialized route key bytes
     * @return The deserialized route key
     * @throws IOException If deserialization fails
     */
    @SuppressWarnings("unchecked")
    private RouteT deserializeRouteKey(byte[] routeBytes) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(routeBytes);
                final ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (RouteT) ois.readObject();
        } catch (Exception e) {
            throw new IOException("Failed to deserialize route key", e);
        }
    }
}
