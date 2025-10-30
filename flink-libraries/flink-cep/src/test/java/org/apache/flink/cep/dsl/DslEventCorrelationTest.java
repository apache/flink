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
import org.apache.flink.cep.dsl.model.StockEvent;
import org.apache.flink.cep.dsl.util.DslTestDataSets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for DSL event correlation (cross-event references). */
public class DslEventCorrelationTest extends AbstractTestBaseJUnit4 {

    @Test
    public void testSingleEventReference() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 105.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                110.0,
                                1500,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "START(price > 100) -> INCREASE(price > START.price)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match -> {
                            double startPrice = match.get("START").get(0).getPrice();
                            double increasePrice = match.get("INCREASE").get(0).getPrice();
                            return startPrice + "->" + increasePrice;
                        });

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        // Should match 105->110
        assertTrue(results.size() >= 1);
        assertTrue(results.stream().anyMatch(r -> r.contains("105.0") && r.contains("110.0")));
    }

    @Test
    public void testChainedReferences() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 105.0, 1100, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                111.0,
                                1200,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "A(price > 0) -> B(price > A.price) -> C(price > B.price)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match -> {
                            double a = match.get("A").get(0).getPrice();
                            double b = match.get("B").get(0).getPrice();
                            double c = match.get("C").get(0).getPrice();
                            return a + "->" + b + "->" + c;
                        });

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertTrue(results.get(0).contains("100.0"));
        assertTrue(results.get(0).contains("105.0"));
        assertTrue(results.get(0).contains("111.0"));
    }

    @Test
    public void testComplexCorrelation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromCollection(DslTestDataSets.eventCorrelationDataset());

        String dslExpression =
                "START(price > 0) -> INCREASE(price > START.price and volume > START.volume)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<StockEvent> result = patternStream.select(match -> match.get("INCREASE").get(0));

        List<StockEvent> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        // Verify that matched events have both price and volume increases
        assertTrue(results.stream().allMatch(e -> e.getPrice() > 100.0 && e.getVolume() > 1000));
    }

    @Test
    public void testMultiAttributeReference() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 105.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 5.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                110.0,
                                1500,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                10.0));

        String dslExpression = "A(price > 0) -> B((price > A.price) and (volume > A.volume))";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<StockEvent> result = patternStream.select(match -> match.get("B").get(0));

        List<StockEvent> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
        assertTrue(results.stream().allMatch(e -> e.getPrice() > 100.0 && e.getVolume() > 1000));
    }

    @Test
    public void testVolumeIncrease() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 101.0, 1500, DslTestDataSets.ts(1), "NASDAQ", 1.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                102.0,
                                2000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                2.0));

        String dslExpression = "LOW(volume > 0) -> HIGH(volume > LOW.volume)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Long> result =
                patternStream.select(match -> match.get("HIGH").get(0).getVolume());

        List<Long> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
        assertTrue(results.stream().allMatch(v -> v > 1000));
    }
}
