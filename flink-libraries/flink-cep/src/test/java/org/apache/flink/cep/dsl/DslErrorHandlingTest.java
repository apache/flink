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

import org.apache.flink.cep.dsl.api.DslCompiler;
import org.apache.flink.cep.dsl.exception.DslCompilationException;
import org.apache.flink.cep.dsl.model.StockEvent;
import org.apache.flink.cep.dsl.util.DslTestDataSets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for DSL error handling. */
public class DslErrorHandlingTest extends AbstractTestBaseJUnit4 {

    @Test
    public void testSyntaxError() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = "TRADE(price >>>)"; // Invalid syntax

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException");
        } catch (DslCompilationException e) {
            assertTrue(e.getMessage().contains("Syntax error") || e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testMissingWhereClause() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = "TRADE symbol = 'AAPL'"; // Missing WHERE

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException");
        } catch (DslCompilationException e) {
            assertTrue(
                    e.getMessage().contains("WHERE")
                            || e.getMessage().contains("Syntax")
                            || e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testEmptyPattern() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = ""; // Empty string

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException or IllegalArgumentException");
        } catch (Exception e) {
            assertTrue(
                    e instanceof DslCompilationException
                            || e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInvalidOperator() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = "TRADE(price <> 150)"; // Invalid operator

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException");
        } catch (DslCompilationException e) {
            assertTrue(e.getMessage().contains("Syntax") || e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testUnbalancedParentheses() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = "TRADE((price > 100)"; // Missing closing paren

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException");
        } catch (DslCompilationException e) {
            assertTrue(
                    e.getMessage().contains("Syntax")
                            || e.getMessage().contains("parse")
                            || e.getMessage().contains("parenthes"));
        }
    }

    @Test
    public void testInvalidQuantifier() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String invalidDsl = "TRADE{-1}(price > 0)"; // Negative quantifier

        try {
            DslCompiler.compile(invalidDsl, input);
            fail("Expected DslCompilationException");
        } catch (DslCompilationException e) {
            assertTrue(
                    e.getMessage().contains("quantifier")
                            || e.getMessage().contains("Syntax")
                            || e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testValidPatternNoError() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockEvent> input =
                env.fromData(
                        new StockEvent("AAPL", "TRADE", 150.0, 1000, DslTestDataSets.ts(0), "NASDAQ", 0.0));

        String validDsl = "TRADE(price > 100)"; // Valid DSL

        // Should not throw exception
        DslCompiler.compile(validDsl, input);
    }
}
