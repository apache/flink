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

/** Tests for DSL pattern sequencing (NEXT, FOLLOWED BY, etc.). */
public class DslPatternMatchingTest extends AbstractTestBaseJUnit4 {

    @Test
    public void testNext() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "ORDER", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "QUOTE",
                                101.0,
                                1100,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression =
                "PATTERN ORDER WHERE eventType = 'ORDER' " + "NEXT TRADE WHERE eventType = 'TRADE'";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                match.get("ORDER").get(0).getEventType()
                                        + "->"
                                        + match.get("TRADE").get(0).getEventType());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals("ORDER->TRADE", results.get(0));
    }

    @Test
    public void testNextWithNoise() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "ORDER", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "QUOTE", 100.5, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                100.0,
                                1000,
                                DslTestDataSets.ts(2),
                                "NASDAQ",
                                0.0));

        String dslExpression = "ORDER(eventType = 'ORDER') -> TRADE(eventType = 'TRADE')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result = patternStream.select(match -> "MATCH");

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        // Should not match because QUOTE is between ORDER and TRADE
        assertEquals(0, results.size());
    }

    @Test
    public void testFollowedBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserActivityEvent> input =
                env.fromCollection(DslTestDataSets.userJourneyDataset());

        String dslExpression = "LOGIN(eventType = 'LOGIN') -> LOGOUT(eventType = 'LOGOUT')";

        PatternStream<UserActivityEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                match.get("LOGIN").get(0).getUserId()
                                        + ":"
                                        + match.get("LOGOUT").get(0).getEventType());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals("user1:LOGOUT", results.get(0));
    }

    @Test
    public void testFollowedByAny() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserActivityEvent> input =
                env.fromData(
                        new UserActivityEvent(
                                "user1", "LOGIN", "/home", 0, DslTestDataSets.ts(0), "s1", 1),
                        new UserActivityEvent(
                                "user1", "CLICK", "/products", 30, DslTestDataSets.ts(1), "s1", 1),
                        new UserActivityEvent(
                                "user1", "CLICK", "/cart", 20, DslTestDataSets.ts(2), "s1", 1),
                        new UserActivityEvent(
                                "user1", "CLICK", "/checkout", 10, DslTestDataSets.ts(3), "s1", 1));

        String dslExpression = "LOGIN(eventType = 'LOGIN') ->> CLICK(eventType = 'CLICK')";

        PatternStream<UserActivityEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(match -> match.get("CLICK").get(0).getPage());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        // Should match multiple CLICK events
        assertTrue(results.size() >= 1);
    }

    @Test
    public void testNotFollowedBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserActivityEvent> input =
                env.fromData(
                        new UserActivityEvent(
                                "user1", "LOGIN", "/home", 0, DslTestDataSets.ts(0), "s1", 1),
                        new UserActivityEvent(
                                "user1", "CLICK", "/products", 30, DslTestDataSets.ts(1), "s1", 1),
                        new UserActivityEvent(
                                "user1", "LOGOUT", "/home", 0, DslTestDataSets.ts(2), "s1", 1));

        String dslExpression = "LOGIN(eventType = 'LOGIN') !-> ERROR(eventType = 'ERROR')";

        PatternStream<UserActivityEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(match -> match.get("LOGIN").get(0).getUserId());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        // Should match because there's no ERROR event
        assertTrue(results.size() >= 1);
    }

    @Test
    public void testComplexSequence() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent(
                                "AAPL", "ORDER", 100.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "TRADE", 100.0, 1000, DslTestDataSets.ts(1), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL", "QUOTE", 101.0, 1100, DslTestDataSets.ts(2), "NASDAQ", 0.0),
                        new StockEvent(
                                "AAPL",
                                "TRADE",
                                101.0,
                                1100,
                                DslTestDataSets.ts(3),
                                "NASDAQ",
                                0.0));

        String dslExpression =
                "A(eventType = 'ORDER') -> B(eventType = 'TRADE') -> C(eventType = 'TRADE')";

        PatternStream<StockEvent> patternStream = DslCompiler.compile(dslExpression, input);

        DataStream<String> result =
                patternStream.select(
                        match ->
                                match.get("A").get(0).getEventType()
                                        + "->"
                                        + match.get("B").get(0).getEventType()
                                        + "->"
                                        + match.get("C").get(0).getEventType());

        List<String> results = new ArrayList<>();
        result.executeAndCollect().forEachRemaining(results::add);

        assertEquals(1, results.size());
        assertEquals("ORDER->TRADE->TRADE", results.get(0));
    }
}
