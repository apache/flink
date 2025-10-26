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

package org.apache.flink.cep.dsl.util;

import org.apache.flink.cep.dsl.model.SensorEvent;
import org.apache.flink.cep.dsl.model.StockEvent;
import org.apache.flink.cep.dsl.model.UserActivityEvent;

import java.util.Arrays;
import java.util.List;

/** Predefined test datasets for DSL testing. */
public class DslTestDataSets {

    private static final long BASE_TIMESTAMP = 1000000000L;

    public static long ts(int offset) {
        return BASE_TIMESTAMP + offset * 1000L;
    }

    /** Dataset for testing comparison operators. */
    public static List<StockEvent> comparisonOperatorDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 150.0, 1000, ts(0), "NASDAQ", 1.5),
                new StockEvent("AAPL", "TRADE", 155.0, 1200, ts(1), "NASDAQ", 3.3),
                new StockEvent("AAPL", "TRADE", 152.0, 800, ts(2), "NASDAQ", -1.9),
                new StockEvent("GOOGL", "TRADE", 2800.0, 500, ts(3), "NASDAQ", 0.5),
                new StockEvent("AAPL", "TRADE", 160.0, 1500, ts(4), "NASDAQ", 5.2));
    }

    /** Dataset for testing logical operators. */
    public static List<StockEvent> logicalOperatorDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 95.0, 900, ts(0), "NASDAQ", -2.0),
                new StockEvent("AAPL", "TRADE", 105.0, 1100, ts(1), "NASDAQ", 2.5),
                new StockEvent("GOOGL", "TRADE", 110.0, 800, ts(2), "NYSE", 1.0),
                new StockEvent("MSFT", "TRADE", 90.0, 1200, ts(3), "NASDAQ", 0.5),
                new StockEvent("AAPL", "TRADE", 120.0, 1500, ts(4), "NASDAQ", 6.0));
    }

    /** Dataset for testing quantifiers. */
    public static List<StockEvent> quantifierDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 100.0, 1000, ts(0), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 105.0, 1100, ts(1), "NASDAQ", 5.0),
                new StockEvent("AAPL", "TRADE", 110.0, 1200, ts(2), "NASDAQ", 10.0),
                new StockEvent("GOOGL", "QUOTE", 2800.0, 500, ts(3), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 115.0, 1300, ts(4), "NASDAQ", 15.0));
    }

    /** Dataset with temperature anomaly pattern. */
    public static List<SensorEvent> temperatureAnomalyDataset() {
        return Arrays.asList(
                new SensorEvent("TEMP_001", "TEMPERATURE", 20.0, "CELSIUS", ts(0), "ZONE_A", "NORMAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 25.0, "CELSIUS", ts(1), "ZONE_A", "NORMAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 35.0, "CELSIUS", ts(2), "ZONE_A", "WARNING"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 45.0, "CELSIUS", ts(3), "ZONE_A", "CRITICAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 22.0, "CELSIUS", ts(4), "ZONE_A", "NORMAL"));
    }

    /** Dataset for user journey testing. */
    public static List<UserActivityEvent> userJourneyDataset() {
        return Arrays.asList(
                new UserActivityEvent("user1", "LOGIN", "/home", 0, ts(0), "session1", 1),
                new UserActivityEvent("user1", "CLICK", "/products", 30, ts(1), "session1", 1),
                new UserActivityEvent("user1", "CLICK", "/cart", 20, ts(2), "session1", 1),
                new UserActivityEvent("user1", "PURCHASE", "/checkout", 60, ts(3), "session1", 1),
                new UserActivityEvent("user1", "LOGOUT", "/home", 0, ts(4), "session1", 1));
    }

    /** Dataset for event correlation testing. */
    public static List<StockEvent> eventCorrelationDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 100.0, 1000, ts(0), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 105.0, 1200, ts(1), "NASDAQ", 5.0),
                new StockEvent("AAPL", "TRADE", 110.0, 1500, ts(2), "NASDAQ", 10.0),
                new StockEvent("AAPL", "TRADE", 115.0, 1800, ts(3), "NASDAQ", 15.0),
                new StockEvent("AAPL", "TRADE", 108.0, 900, ts(4), "NASDAQ", 8.0));
    }

    /** Dataset for pattern sequencing testing. */
    public static List<StockEvent> patternSequencingDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "ORDER", 100.0, 1000, ts(0), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 100.0, 1000, ts(1), "NASDAQ", 0.0),
                new StockEvent("GOOGL", "QUOTE", 2800.0, 500, ts(2), "NASDAQ", 0.0),
                new StockEvent("AAPL", "QUOTE", 101.0, 1100, ts(3), "NASDAQ", 1.0),
                new StockEvent("AAPL", "TRADE", 102.0, 1200, ts(4), "NASDAQ", 2.0));
    }

    /** Dataset for skip strategy testing. */
    public static List<StockEvent> skipStrategyDataset() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 100.0, 1000, ts(0), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 101.0, 1100, ts(1), "NASDAQ", 1.0),
                new StockEvent("AAPL", "TRADE", 102.0, 1200, ts(2), "NASDAQ", 2.0),
                new StockEvent("AAPL", "TRADE", 103.0, 1300, ts(3), "NASDAQ", 3.0),
                new StockEvent("AAPL", "TRADE", 104.0, 1400, ts(4), "NASDAQ", 4.0));
    }

    /** Stock dataset with price increase pattern. */
    public static List<StockEvent> priceIncreasePattern() {
        return Arrays.asList(
                new StockEvent("AAPL", "TRADE", 150.0, 1000, ts(0), "NASDAQ", 0.0),
                new StockEvent("AAPL", "TRADE", 152.0, 1100, ts(1), "NASDAQ", 1.3),
                new StockEvent("AAPL", "TRADE", 155.0, 1200, ts(2), "NASDAQ", 3.3),
                new StockEvent("AAPL", "TRADE", 158.0, 1300, ts(3), "NASDAQ", 5.3),
                new StockEvent("AAPL", "TRADE", 160.0, 1400, ts(4), "NASDAQ", 6.7));
    }

    /** Sensor dataset with escalating values. */
    public static List<SensorEvent> escalatingValues() {
        return Arrays.asList(
                new SensorEvent("TEMP_001", "TEMPERATURE", 25.0, "CELSIUS", ts(0), "ZONE_A", "NORMAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 30.0, "CELSIUS", ts(1), "ZONE_A", "NORMAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 38.0, "CELSIUS", ts(2), "ZONE_A", "WARNING"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 46.0, "CELSIUS", ts(3), "ZONE_A", "CRITICAL"),
                new SensorEvent("TEMP_001", "TEMPERATURE", 50.0, "CELSIUS", ts(4), "ZONE_A", "CRITICAL"));
    }
}
