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

/** Tests for DSL quantifiers (*, +, ?, {n}, {n,m}). */
public class DslQuantifierTest extends AbstractTestBaseJUnit4 {

    @Test
    public void testOneOrMore() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 95.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 105.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 110.0, 1000, DslTestDataSets.ts(2), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 90.0, 1000, DslTestDataSets.ts(3), "NASDAQ", 0.0));

        String dslExpression = "TRADE+(price > 100)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<List<StockEvent>> result =
                patternStream.select(match -> match.get("TRADE"));

        List<List<StockEvent>> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        assertTrue(results.stream().anyMatch(list -> list.size() >= 1));
    }

    @Test
    public void testOptional() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 800, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 100.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 100.0, 500, DslTestDataSets.ts(2), "NASDAQ", 0.0));

        String dslExpression = "TRADE?(volume > 1000)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result = patternStream.select(match -> "MATCH");

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
    }

    @Test
    public void testExactCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 101.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 102.0, 1000, DslTestDataSets.ts(2), "NASDAQ", 0.0),
                        new StockEvent("GOOGL", "TRADE", 2800.0, 500, DslTestDataSets.ts(3), "NASDAQ", 0.0));

        String dslExpression = "TRADE{3}(symbol = 'AAPL')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<List<StockEvent>> result =
                patternStream.select(match -> match.get("TRADE"));

        List<List<StockEvent>> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals(3, results.get(0).size());
    }

    @Test
    public void testRangeCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 101.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 102.0, 1000, DslTestDataSets.ts(2), "NASDAQ", 0.0),
                        new StockEvent("GOOGL", "QUOTE", 2800.0, 500, DslTestDataSets.ts(3), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 103.0, 1000, DslTestDataSets.ts(4), "NASDAQ", 0.0));

        String dslExpression = "TRADE{2,4}(eventType = 'TRADE')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<List<StockEvent>> result =
                patternStream.select(match -> match.get("TRADE"));

        List<List<StockEvent>> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        assertTrue(results.stream().allMatch(list -> list.size() >= 2 && list.size() <= 4));
    }

    @Test
    public void testQuantifierWithCondition() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input = env.fromCollection(DslTestDataSets.quantifierDataset());

        String dslExpression = "TRADE+(symbol = 'AAPL')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<List<StockEvent>> result =
                patternStream.select(match -> match.get("TRADE"));

        List<List<StockEvent>> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        assertTrue(
                results.stream()
                        .allMatch(list -> list.stream().allMatch(e -> e.getSymbol().equals("AAPL"))));
    }
}
