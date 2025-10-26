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

package org.apache.flink.cep.dsl;

import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.dsl.api.DslCompiler;
import org.apache.flink.cep.dsl.api.EventAdapter;
import org.apache.flink.cep.dsl.util.MapEventAdapter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Example usage of the CEP DSL.
 *
 * <p>This class demonstrates various ways to use the DSL with different event types and patterns.
 * These are examples only and not executable tests.
 */
public class DslExampleUsage {

    // Example 1: Simple POJO events with basic pattern
    public static void simplePojoExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create sample data
        DataStream<SensorReading> sensorData =
                env.fromElements(
                        new SensorReading("sensor1", 95.0, System.currentTimeMillis()),
                        new SensorReading("sensor1", 105.0, System.currentTimeMillis()),
                        new SensorReading("sensor1", 110.0, System.currentTimeMillis()));

        // Define pattern using DSL
        PatternStream<SensorReading> pattern =
                DslCompiler.compile("HighTemp(temperature > 100)", sensorData);

        // Process matches
        pattern.select(
                        match -> {
                            SensorReading reading = match.get("HighTemp").get(0);
                            return String.format(
                                    "High temperature alert: Sensor %s at %.1f°C",
                                    reading.id, reading.temperature);
                        })
                .print();

        env.execute("Simple POJO Example");
    }

    // Example 2: Event correlation
    public static void eventCorrelationExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> sensorData =
                env.fromElements(
                        new SensorReading("sensor1", 95.0, 1000L),
                        new SensorReading("sensor1", 105.0, 2000L),
                        new SensorReading("sensor2", 110.0, 3000L));

        // Pattern with event correlation
        String dsl = "Start(id = 'sensor1' and temperature > 90) -> " +
                    "End(id = Start.id and temperature > Start.temperature)";

        PatternStream<SensorReading> pattern = DslCompiler.compile(dsl, sensorData);

        pattern.select(
                        match -> {
                            SensorReading start = match.get("Start").get(0);
                            SensorReading end = match.get("End").get(0);
                            return String.format(
                                    "Temperature rise detected: %.1f -> %.1f",
                                    start.temperature, end.temperature);
                        })
                .print();

        env.execute("Event Correlation Example");
    }

    // Example 3: Map-based events
    public static void mapEventExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create Map events
        Map<String, Object> event1 = new HashMap<>();
        event1.put("_eventType", "Alert");
        event1.put("severity", 7);
        event1.put("message", "High CPU usage");

        Map<String, Object> event2 = new HashMap<>();
        event2.put("_eventType", "Alert");
        event2.put("severity", 9);
        event2.put("message", "Critical error");

        DataStream<Map<String, Object>> alerts = env.fromElements(event1, event2);

        // Use MapEventAdapter
        PatternStream<Map<String, Object>> pattern =
                DslCompiler.compile(
                        "Alert(severity > 5)", alerts, new MapEventAdapter());

        pattern.select(match -> match.get("Alert").get(0).get("message")).print();

        env.execute("Map Event Example");
    }

    // Example 4: Complex pattern with quantifiers and time window
    public static void complexPatternExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> userEvents =
                env.fromElements(
                        new UserEvent("user1", "login", 1000L),
                        new UserEvent("user1", "browse", 2000L),
                        new UserEvent("user1", "browse", 3000L),
                        new UserEvent("user1", "purchase", 4000L));

        // Complex pattern: login -> multiple browses -> purchase, within 30 seconds
        String dsl =
                "%SKIP_TO_LAST['Login'] "
                        + "Login(action = 'login') -> "
                        + "Browse{1,5}(action = 'browse' and userId = Login.userId) -> "
                        + "Purchase(action = 'purchase' and userId = Login.userId) "
                        + "within 30s";

        PatternStream<UserEvent> pattern = DslCompiler.compile(dsl, userEvents);

        pattern.select(
                        match -> {
                            UserEvent login = match.get("Login").get(0);
                            List<UserEvent> browses = match.get("Browse");
                            UserEvent purchase = match.get("Purchase").get(0);
                            return String.format(
                                    "User %s: login -> %d browses -> purchase",
                                    login.userId, browses.size());
                        })
                .print();

        env.execute("Complex Pattern Example");
    }

    // Example 5: Builder API with custom adapter
    public static void builderApiExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<CustomEvent> events = env.fromElements(new CustomEvent());

        // Custom event adapter
        EventAdapter<CustomEvent> customAdapter =
                new EventAdapter<CustomEvent>() {
                    @Override
                    public java.util.Optional<Object> getAttribute(
                            CustomEvent event, String attributeName) {
                        return java.util.Optional.ofNullable(event.getField(attributeName));
                    }

                    @Override
                    public String getEventType(CustomEvent event) {
                        return event.getTypeName();
                    }
                };

        // Use builder API
        PatternStream<CustomEvent> pattern =
                DslCompiler.<CustomEvent>builder()
                        .withStrictTypeMatching()
                        .withEventAdapter(customAdapter)
                        .compile("MyEvent(value > 100)", events);

        pattern.select(match -> "Matched: " + match.get("MyEvent").get(0)).print();

        env.execute("Builder API Example");
    }

    // Example event classes

    /** Simple sensor reading POJO. */
    public static class SensorReading {
        public String id;
        public double temperature;
        public long timestamp;

        public SensorReading(String id, double temperature, long timestamp) {
            this.id = id;
            this.temperature = temperature;
            this.timestamp = timestamp;
        }

        public String getId() {
            return id;
        }

        public double getTemperature() {
            return temperature;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /** User event POJO. */
    public static class UserEvent {
        public String userId;
        public String action;
        public long timestamp;

        public UserEvent(String userId, String action, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public String getAction() {
            return action;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /** Custom event type. */
    public static class CustomEvent {
        public Object getField(String name) {
            return null; // Implementation omitted
        }

        public String getTypeName() {
            return "MyEvent";
        }
    }
}
