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

/** Tests for DSL expression evaluation (comparison and logical operators). */
public class DslExpressionEvaluationTest extends AbstractTestBaseJUnit4 {

    @Test
    public void testEqualsOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 155.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 150.0, 800, DslTestDataSets.ts(2), "NASDAQ", 0.0));

        String dslExpression = "TRADE(price = 150.0)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Double> result =
                patternStream.select(match -> match.get("TRADE").get(0).getPrice());

        List<Double> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(p -> p == 150.0));
    }

    @Test
    public void testNotEqualsOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "GOOGL",
                                "TRADE",
                                100.0,
                                1000,
                                DslTestDataSets.ts(1),
                                "NASDAQ",
                                0.0),
                        new StockEvent(
                                "MSFT",
                                "TRADE",
                                100.0,
                                1000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(symbol != 'AAPL')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(match -> match.get("TRADE").get(0).getSymbol());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.contains("GOOGL"));
        assertTrue(results.contains("MSFT"));
    }

    @Test
    public void testLessThanOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 800, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 500, DslTestDataSets.ts(2), "NASDAQ", 0.0));

        String dslExpression = "TRADE(volume < 1000)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Long> result =
                patternStream.select(match -> match.get("TRADE").get(0).getVolume());

        List<Long> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(v -> v < 1000));
    }

    @Test
    public void testLessThanOrEqual() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 800, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                100.0,
                                1200,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(volume <= 1000)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Long> result =
                patternStream.select(match -> match.get("TRADE").get(0).getVolume());

        List<Long> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(v -> v <= 1000));
    }

    @Test
    public void testGreaterThanOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 95.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 105.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                110.0,
                                1000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(price > 100.0)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Double> result =
                patternStream.select(match -> match.get("TRADE").get(0).getPrice());

        List<Double> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(p -> p > 100.0));
    }

    @Test
    public void testGreaterThanOrEqual() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 95.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                105.0,
                                1000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(price >= 100.0)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<Double> result =
                patternStream.select(match -> match.get("TRADE").get(0).getPrice());

        List<Double> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(p -> p >= 100.0));
    }

    @Test
    public void testStringEquality() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromCollection(DslTestDataSets.comparisonOperatorDataset());

        String dslExpression = "TRADE(symbol = 'AAPL')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(match -> match.get("TRADE").get(0).getSymbol());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(4, results.size());
        assertTrue(results.stream().allMatch(s -> s.equals("AAPL")));
    }

    @Test
    public void testAndOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input = env.fromCollection(DslTestDataSets.logicalOperatorDataset());

        String dslExpression = "TRADE(price > 100 and volume > 1000)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<StockEvent> result = patternStream.select(match -> match.get("TRADE").get(0));

        List<StockEvent> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(e -> e.getPrice() > 100 && e.getVolume() > 1000));
    }

    @Test
    public void testOrOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "GOOGL",
                                "TRADE",
                                100.0,
                                1000,
                                DslTestDataSets.ts(1),
                                "NASDAQ",
                                0.0),
                        new StockEvent(
                                "MSFT",
                                "TRADE",
                                100.0,
                                1000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "TRADE(symbol = 'AAPL' or symbol = 'GOOGL')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(match -> match.get("TRADE").get(0).getSymbol());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(2, results.size());
        assertTrue(results.contains("AAPL"));
        assertTrue(results.contains("GOOGL"));
    }

    @Test
    public void testComplexLogic() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input = env.fromCollection(DslTestDataSets.logicalOperatorDataset());

        String dslExpression = "TRADE((price > 100 and volume > 1000) or change > 5.0)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<StockEvent> result = patternStream.select(match -> match.get("TRADE").get(0));

        List<StockEvent> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(3, results.size());
        assertTrue(
                results.stream()
                        .allMatch(
                                e ->
                                        (e.getPrice() > 100 && e.getVolume() > 1000)
                                                || e.getChange() > 5.0));
    }
}
