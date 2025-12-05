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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DemultiplexingSinkStateSerializer}. */
class DemultiplexingSinkStateSerializerTest {

    @Test
    void testSerializeDeserializeEmptyState() throws IOException {
        final DemultiplexingSinkStateSerializer<String> serializer =
                new DemultiplexingSinkStateSerializer<>();
        final DemultiplexingSinkState<String> originalState = new DemultiplexingSinkState<>();

        final byte[] serialized = serializer.serialize(originalState);
        final DemultiplexingSinkState<String> deserializedState =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedState).isEqualTo(originalState);
        assertThat(deserializedState.isEmpty()).isTrue();
    }

    @Test
    void testSerializeDeserializeStateWithRoutes() throws IOException {
        final DemultiplexingSinkStateSerializer<String> serializer =
                new DemultiplexingSinkStateSerializer<>();

        // Create state with multiple routes ("route3" -> empty state)
        final Map<String, byte[]> routeStates = new HashMap<>();
        routeStates.put("route1", new byte[] {1, 2, 3});
        routeStates.put("route2", new byte[] {4, 5, 6, 7});
        routeStates.put("route3", new byte[0]);

        final DemultiplexingSinkState<String> originalState =
                new DemultiplexingSinkState<>(routeStates);

        final byte[] serialized = serializer.serialize(originalState);
        final DemultiplexingSinkState<String> deserializedState =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedState).isEqualTo(originalState);
        assertThat(deserializedState.getRoutes())
                .containsExactlyInAnyOrder("route1", "route2", "route3");
        assertThat(deserializedState.getRouteState("route1")).containsExactly(1, 2, 3);
        assertThat(deserializedState.getRouteState("route2")).containsExactly(4, 5, 6, 7);
        assertThat(deserializedState.getRouteState("route3")).isEmpty();
    }

    @Test
    void testSerializeDeserializeWithComplexRouteKeys() throws IOException {
        final DemultiplexingSinkStateSerializer<ComplexRouteKey> serializer =
                new DemultiplexingSinkStateSerializer<>();

        // Create state with complex route keys
        final Map<ComplexRouteKey, byte[]> routeStates = new HashMap<>();
        routeStates.put(new ComplexRouteKey("cluster1", 9092), new byte[] {1, 2});
        routeStates.put(new ComplexRouteKey("cluster2", 9093), new byte[] {3, 4});

        final DemultiplexingSinkState<ComplexRouteKey> originalState =
                new DemultiplexingSinkState<>(routeStates);

        final byte[] serialized = serializer.serialize(originalState);
        final DemultiplexingSinkState<ComplexRouteKey> deserializedState =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedState).isEqualTo(originalState);
        assertThat(deserializedState.size()).isEqualTo(2);
    }

    @Test
    void testDeserializeWithWrongVersion() {
        final DemultiplexingSinkStateSerializer<String> serializer =
                new DemultiplexingSinkStateSerializer<>();
        final byte[] serialized = new byte[] {1, 2, 3, 4};

        assertThatThrownBy(() -> serializer.deserialize(999, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported version: 999");
    }

    @Test
    void testGetVersion() {
        final DemultiplexingSinkStateSerializer<String> serializer =
                new DemultiplexingSinkStateSerializer<>();

        assertThat(serializer.getVersion()).isEqualTo(1);
    }

    @Test
    void testSetRouteStateWithNullState() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();

        // Add a route first
        state.setRouteState("route1", new byte[] {1, 2, 3});
        assertThat(state.getRoutes()).containsExactly("route1");

        // Setting null state should remove the route
        state.setRouteState("route1", null);
        assertThat(state.getRoutes()).isEmpty();
        assertThat(state.getRouteState("route1")).isNull();
    }

    @Test
    void testSetRouteStateWithNullStateOnNonExistentRoute() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();

        // Setting null state on non-existent route should be no-op
        state.setRouteState("nonExistent", null);
        assertThat(state.getRoutes()).isEmpty();
    }

    @Test
    void testEqualsWithSameInstance() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();
        state.setRouteState("route1", new byte[] {1, 2, 3});

        assertThat(state.equals(state)).isTrue();
    }

    @Test
    void testEqualsWithNull() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();

        assertThat(state.equals(null)).isFalse();
    }

    @Test
    void testEqualsWithDifferentClass() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();

        assertThat(state.equals("not a state")).isFalse();
    }

    @Test
    void testEqualsWithDifferentRouteSizes() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("route1", new byte[] {1, 2, 3});

        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("route1", new byte[] {1, 2, 3});
        state2.setRouteState("route2", new byte[] {4, 5, 6});

        assertThat(state1.equals(state2)).isFalse();
        assertThat(state2.equals(state1)).isFalse();
    }

    @Test
    void testEqualsWithSameRoutesButDifferentStates() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("route1", new byte[] {1, 2, 3});

        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("route1", new byte[] {4, 5, 6});

        assertThat(state1.equals(state2)).isFalse();
    }

    @Test
    void testEqualsWithSameRoutesAndStates() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("route1", new byte[] {1, 2, 3});
        state1.setRouteState("route2", new byte[] {4, 5, 6});

        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("route1", new byte[] {1, 2, 3});
        state2.setRouteState("route2", new byte[] {4, 5, 6});

        assertThat(state1.equals(state2)).isTrue();
        assertThat(state2.equals(state1)).isTrue();
    }

    @Test
    void testHashCodeConsistency() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("route1", new byte[] {1, 2, 3});
        state1.setRouteState("route2", new byte[] {4, 5, 6});

        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("route1", new byte[] {1, 2, 3});
        state2.setRouteState("route2", new byte[] {4, 5, 6});

        // Equal objects must have equal hash codes
        assertThat(state1.equals(state2)).isTrue();
        assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
    }

    @Test
    void testHashCodeWithEmptyState() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();

        assertThat(state1.hashCode()).isEqualTo(state2.hashCode());
    }

    @Test
    void testHashCodeWithDifferentStates() {
        DemultiplexingSinkState<String> state1 = new DemultiplexingSinkState<>();
        state1.setRouteState("route1", new byte[] {1, 2, 3});

        DemultiplexingSinkState<String> state2 = new DemultiplexingSinkState<>();
        state2.setRouteState("route1", new byte[] {4, 5, 6});

        // Different objects should typically have different hash codes
        assertThat(state1.hashCode()).isNotEqualTo(state2.hashCode());
    }

    @Test
    void testToStringWithEmptyState() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();

        String toString = state.toString();
        assertThat(toString).contains("DemultiplexingSinkState");
        assertThat(toString).contains("routeCount=0");
        assertThat(toString).contains("routes=[]");
    }

    @Test
    void testToStringWithSingleRoute() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();
        state.setRouteState("route1", new byte[] {1, 2, 3});

        String toString = state.toString();
        assertThat(toString).contains("DemultiplexingSinkState");
        assertThat(toString).contains("routeCount=1");
        assertThat(toString).contains("route1");
    }

    @Test
    void testToStringWithMultipleRoutes() {
        DemultiplexingSinkState<String> state = new DemultiplexingSinkState<>();
        state.setRouteState("route1", new byte[] {1, 2, 3});
        state.setRouteState("route2", new byte[] {4, 5, 6});

        String toString = state.toString();
        assertThat(toString).contains("DemultiplexingSinkState");
        assertThat(toString).contains("routeCount=2");
        assertThat(toString).contains("route1");
        assertThat(toString).contains("route2");
    }

    /** A complex route key for testing serialization. */
    private static class ComplexRouteKey implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private final String host;
        private final int port;

        public ComplexRouteKey(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ComplexRouteKey that = (ComplexRouteKey) o;
            return port == that.port && java.util.Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(host, port);
        }

        @Override
        public String toString() {
            return "ComplexRouteKey{host='" + host + "', port=" + port + '}';
        }
    }
}
