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
import org.apache.flink.cep.dsl.model.SensorEvent;
import org.apache.flink.cep.dsl.model.StockEvent;
import org.apache.flink.cep.dsl.model.UserActivityEvent;
import org.apache.flink.cep.dsl.util.DslTestDataSets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** End-to-end tests for complex DSL scenarios. */
public class DslCompilerE2ETest extends AbstractTestBaseJUnit4 {

    @Test
    public void testComplexFinancialPattern() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input = env.fromCollection(DslTestDataSets.priceIncreasePattern());

        // Complex DSL: Detect sustained price increase with volume
        String dslExpression =
                "START((symbol = 'AAPL') and (price > 150) and (volume > 1000)) "
                        + "-> INCREASE1(price > START.price) "
                        + "-> INCREASE2(price > INCREASE1.price)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match -> {
                            StockEvent start = match.get("START").get(0);
                            StockEvent increase2 = match.get("INCREASE2").get(0);
                            double priceGain =
                                    ((increase2.getPrice() - start.getPrice()) / start.getPrice())
                                            * 100;
                            return start.getSymbol() + ":" + String.format("%.2f", priceGain);
                        });

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        assertTrue(results.stream().anyMatch(r -> r.startsWith("AAPL:")));
    }

    @Test
    public void testIoTAnomalyDetection() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorEvent> input = env.fromCollection(DslTestDataSets.escalatingValues());

        String dslExpression =
                "NORMAL((status = 'NORMAL') and (value < 35)) "
                        + "-> WARNING((status = 'WARNING') and (value > NORMAL.value)) "
                        + "-> CRITICAL((status = 'CRITICAL') and (value > WARNING.value))";

        PatternStream<SensorEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                "ANOMALY:"
                                        + match.get("NORMAL").get(0).getValue()
                                        + "->"
                                        + match.get("WARNING").get(0).getValue()
                                        + "->"
                                        + match.get("CRITICAL").get(0).getValue());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() > 0);
        assertTrue(results.stream().anyMatch(r -> r.startsWith("ANOMALY:")));
    }

    @Test
    public void testUserJourneyFunnel() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserActivityEvent> input = env.fromCollection(DslTestDataSets.userJourneyDataset());

        String dslExpression =
                "LOGIN(eventType = 'LOGIN') "
                        + "-> CLICK((eventType = 'CLICK') and (duration > 10)) "
                        + "-> PURCHASE((eventType = 'PURCHASE') and (count > 0))";

        PatternStream<UserActivityEvent> patternStream =
                DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                "FUNNEL:"
                                        + match.get("LOGIN").get(0).getUserId()
                                        + ":"
                                        + match.get("PURCHASE").get(0).getEventType());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
        assertTrue(results.stream().anyMatch(r -> r.contains("PURCHASE")));
    }

    @Test
    public void testMultiStepPricePattern() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 102.0, 1100, DslTestDataSets.ts(1), "NASDAQ", 2.0),
                        new StockEvent("AAPL", "TRADE", 104.0, 1200, DslTestDataSets.ts(2), "NASDAQ", 4.0),
                        new StockEvent("AAPL", "TRADE", 106.0, 1300, DslTestDataSets.ts(3), "NASDAQ", 6.0));

        String dslExpression =
                "A(price > 0) "
                        + "-> B(price > A.price) "
                        + "-> C(price > B.price) "
                        + "-> D(price > C.price)";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result = patternStream.select(match -> "MATCH");

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
    }

    @Test
    public void testCombinedConditionsPattern() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 105.0, 1200, DslTestDataSets.ts(1), "NASDAQ", 5.0),
                        new StockEvent("GOOGL", "TRADE", 110.0, 800, DslTestDataSets.ts(2), "NYSE", 1.0),
                        new StockEvent("AAPL", "TRADE", 110.0, 1500, DslTestDataSets.ts(3), "NASDAQ", 10.0));

        String dslExpression =
                "START((symbol = 'AAPL') and (price > 100) and (volume > 1000)) "
                        + "-> END((symbol = 'AAPL') and (price > START.price) and (volume > START.volume))";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<StockEvent> result = patternStream.select(match -> match.get("END").get(0));

        List<StockEvent> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
        assertTrue(
                results.stream()
                        .allMatch(e -> e.getSymbol().equals("AAPL") && e.getVolume() > 1200));
    }

    @Test
    public void testStringComparisonPattern() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("GOOGL", "TRADE", 100.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(2), "NYSE", 0.0));

        String dslExpression =
                "TRADE((symbol = 'AAPL') and (exchange = 'NASDAQ'))";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                match.get("TRADE").get(0).getSymbol()
                                        + ":"
                                        + match.get("TRADE").get(0).getExchange());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals("AAPL:NASDAQ", results.get(0));
    }

    @Test
    public void testQuantifierWithSequence() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent("AAPL", "TRADE", 105.0, 1100, DslTestDataSets.ts(1), "NASDAQ", 5.0),
                        new StockEvent("AAPL", "TRADE", 110.0, 1200, DslTestDataSets.ts(2), "NASDAQ", 10.0),
                        new StockEvent("AAPL", "QUOTE", 111.0, 1300, DslTestDataSets.ts(3), "NASDAQ", 11.0));

        String dslExpression =
                "TRADES((eventType = 'TRADE') and (symbol = 'AAPL')) "
                        + "-> QUOTE(eventType = 'QUOTE')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                match.get("TRADES").get(0).getEventType()
                                        + "->"
                                        + match.get("QUOTE").get(0).getEventType());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertTrue(results.size() >= 1);
        assertTrue(results.stream().allMatch(r -> r.contains("QUOTE")));
    }
}
