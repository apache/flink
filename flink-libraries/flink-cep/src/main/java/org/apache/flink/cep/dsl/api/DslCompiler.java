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

package org.apache.flink.cep.dsl.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.dsl.exception.DslCompilationException;
import org.apache.flink.cep.dsl.grammar.CepDslLexer;
import org.apache.flink.cep.dsl.grammar.CepDslParser;
import org.apache.flink.cep.dsl.pattern.DslPatternTranslator;
import org.apache.flink.cep.dsl.util.CaseInsensitiveInputStream;
import org.apache.flink.cep.dsl.util.ReflectiveEventAdapter;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main API for compiling DSL expressions into Flink CEP PatternStreams.
 *
 * <p>This class provides static factory methods to compile DSL pattern expressions into {@link
 * PatternStream} objects that can be used with Flink's CEP library.
 *
 * <p><b>Basic Usage:</b>
 *
 * <pre>{@code
 * DataStream<SensorReading> sensorData = ...;
 *
 * // Simple pattern matching
 * PatternStream<SensorReading> pattern = DslCompiler.compile(
 *     "HighTemp(temperature > 100)",
 *     sensorData
 * );
 *
 * // Pattern with event correlation
 * PatternStream<SensorReading> correlatedPattern = DslCompiler.compile(
 *     "Start(id > 0) -> End(id = Start.id and value > 50)",
 *     sensorData
 * );
 * }</pre>
 *
 * <p><b>Advanced Usage with Custom Adapter:</b>
 *
 * <pre>{@code
 * EventAdapter<MyCustomEvent> adapter = new MyCustomAdapter();
 * PatternStream<MyCustomEvent> pattern = DslCompiler.compile(
 *     "Alert(severity > 5)",
 *     customEventStream,
 *     adapter
 * );
 * }</pre>
 *
 * <p><b>Builder API:</b>
 *
 * <pre>{@code
 * PatternStream<Event> pattern = DslCompiler.<Event>builder()
 *     .withStrictTypeMatching()
 *     .withEventAdapter(customAdapter)
 *     .compile("A(x > 10) -> B(y < 5)", dataStream);
 * }</pre>
 *
 * @see DslCompilerBuilder
 * @see EventAdapter
 */
@PublicEvolving
public class DslCompiler {

    private static final Logger LOG = LoggerFactory.getLogger(DslCompiler.class);

    // Private constructor to prevent instantiation
    private DslCompiler() {}

    /**
     * Compile a DSL expression using the default reflective event adapter.
     *
     * <p>This method uses reflection to automatically access POJO fields and getters. It's suitable
     * for most use cases where events are simple Java objects.
     *
     * @param dslExpression The DSL pattern expression to compile
     * @param dataStream The input data stream
     * @param <T> The type of events in the stream
     * @return A PatternStream configured with the compiled pattern
     * @throws DslCompilationException if the DSL expression is invalid
     */
    public static <T> PatternStream<T> compile(String dslExpression, DataStream<T> dataStream) {
        return compile(dslExpression, dataStream, new ReflectiveEventAdapter<>(), false);
    }

    /**
     * Compile a DSL expression with a custom event adapter.
     *
     * <p>Use this method when you need custom logic for extracting attributes from events, or when
     * working with non-POJO event types (e.g., Maps, custom data structures).
     *
     * @param dslExpression The DSL pattern expression to compile
     * @param dataStream The input data stream
     * @param eventAdapter Custom adapter for extracting event attributes
     * @param <T> The type of events in the stream
     * @return A PatternStream configured with the compiled pattern
     * @throws DslCompilationException if the DSL expression is invalid
     */
    public static <T> PatternStream<T> compile(
            String dslExpression, DataStream<T> dataStream, EventAdapter<T> eventAdapter) {
        return compile(dslExpression, dataStream, eventAdapter, false);
    }

    /**
     * Compile a DSL expression with full configuration options.
     *
     * <p>This method provides complete control over DSL compilation, including strict type
     * matching.
     *
     * @param dslExpression The DSL pattern expression to compile
     * @param dataStream The input data stream
     * @param eventAdapter Custom adapter for extracting event attributes
     * @param strictTypeMatching Whether to enforce strict event type matching (event type in DSL
     *     must match actual event type)
     * @param <T> The type of events in the stream
     * @return A PatternStream configured with the compiled pattern
     * @throws DslCompilationException if the DSL expression is invalid
     */
    public static <T> PatternStream<T> compile(
            String dslExpression,
            DataStream<T> dataStream,
            EventAdapter<T> eventAdapter,
            boolean strictTypeMatching) {

        LOG.info("Compiling DSL expression: {}", dslExpression);

        try {
            // Step 1: Parse DSL expression
            Pattern<T, T> pattern = parseDsl(dslExpression, eventAdapter, strictTypeMatching);

            // Step 2: Create PatternStream
            PatternStream<T> patternStream = CEP.pattern(dataStream, pattern).inEventTime();

            LOG.info("Successfully compiled DSL expression into pattern: {}", pattern.getName());
            return patternStream;

        } catch (RecognitionException e) {
            throw new DslCompilationException(
                    "Failed to parse DSL expression: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new DslCompilationException(
                    "Failed to compile DSL expression: " + e.getMessage(), e);
        }
    }

    /**
     * Create a builder for more complex configuration.
     *
     * <p>The builder pattern allows for fluent, readable configuration of DSL compilation options.
     *
     * @param <T> The type of events
     * @return A new DslCompilerBuilder
     */
    public static <T> DslCompilerBuilder<T> builder() {
        return new DslCompilerBuilder<>();
    }

    /**
     * Parse a DSL expression into a Flink Pattern.
     *
     * @param dslExpression The DSL expression to parse
     * @param eventAdapter The event adapter for attribute extraction
     * @param strictTypeMatching Whether to enforce strict type matching
     * @param <T> The event type
     * @return The compiled Pattern
     * @throws DslCompilationException if parsing fails
     */
    static <T> Pattern<T, T> parseDsl(
            String dslExpression, EventAdapter<T> eventAdapter, boolean strictTypeMatching) {

        // Create case-insensitive input stream
        CaseInsensitiveInputStream inputStream = new CaseInsensitiveInputStream(dslExpression);

        // Create lexer and parser
        CepDslLexer lexer = new CepDslLexer(inputStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        CepDslParser parser = new CepDslParser(tokens);

        // Parse the expression
        ParseTree parseTree = parser.startPatternExpressionRule();

        // Check for syntax errors
        if (parser.getNumberOfSyntaxErrors() > 0) {
            throw new DslCompilationException(
                    String.format(
                            "DSL expression contains %d syntax error(s)",
                            parser.getNumberOfSyntaxErrors()));
        }

        // Walk the parse tree and build the pattern
        ParseTreeWalker walker = ParseTreeWalker.DEFAULT;
        DslPatternTranslator<T> translator =
                new DslPatternTranslator<>(eventAdapter, strictTypeMatching);
        walker.walk(translator, parseTree);

        Pattern<T, T> pattern = translator.getPattern();
        if (pattern == null) {
            throw new DslCompilationException(
                    "Failed to translate DSL expression into a valid pattern");
        }

        return pattern;
    }
}
