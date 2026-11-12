/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.codesplit;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStreamRewriter;
import org.antlr.v4.runtime.misc.Interval;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkTokenStreamRewriter}. */
class FlinkTokenStreamRewriterTest {

    private static final String CODE =
            "class A { int a = 1; void f(int x) { if (x > 0) { a = x; } else { return; } } }";

    private static final String[] KINDS = {
        "insertBefore", "insertAfter", "replace", "delete", "getText"
    };

    private static final String FAILED = "failed";

    private static final int SEEDS = 100_000;
    private static final int CHUNKS = 8;
    // a different range of seeds each run, a failure reports its seed
    private static final long OFFSET =
            ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - SEEDS);

    @ParameterizedTest
    @MethodSource("chunks")
    @Execution(ExecutionMode.CONCURRENT)
    void sameOutputAsTokenStreamRewriter(int chunk) {
        for (long seed = OFFSET + (long) chunk * SEEDS / CHUNKS;
                seed < OFFSET + (long) (chunk + 1) * SEEDS / CHUNKS;
                seed++) {
            Random random = new Random(seed);
            CommonTokenStream tokens =
                    new CommonTokenStream(new JavaLexer(CharStreams.fromString(CODE)));
            // fills the token stream, so it goes before tokens.size()
            FlinkTokenStreamRewriter actual = new FlinkTokenStreamRewriter(tokens);
            int size = tokens.size();

            List<Consumer<FlinkTokenStreamRewriter>> actualOps = new ArrayList<>();
            List<Consumer<TokenStreamRewriter>> expectedOps = new ArrayList<>();
            StringBuilder description = new StringBuilder();
            for (int i = random.nextInt(8); i >= 0; i--) {
                // the last step is always a getText
                int kind = i == 0 ? 4 : random.nextInt(KINDS.length);
                if (kind == 4) {
                    int from = random.nextInt(size);
                    Interval interval = Interval.of(from, from + random.nextInt(size - from));
                    description.append(String.format("%ngetText %s", interval));
                    String actualText =
                            apply(
                                    () -> {
                                        actualOps.forEach(op -> op.accept(actual));
                                        actualOps.clear();
                                        return actual.getText(interval);
                                    });
                    // a new TokenStreamRewriter each time, its getText changes its state
                    String expectedText =
                            apply(
                                    () -> {
                                        TokenStreamRewriter expected =
                                                new TokenStreamRewriter(tokens);
                                        expectedOps.forEach(op -> op.accept(expected));
                                        return expected.getText(interval);
                                    });
                    assertThat(actualText)
                            .as("seed %d%s", seed, description)
                            .isEqualTo(expectedText);
                    if (FAILED.equals(actualText)) {
                        break; // actual may be partly updated
                    }
                    continue;
                }
                int start = random.nextInt(size);
                int span = random.nextBoolean() ? random.nextInt(4) : random.nextInt(size - start);
                Token a = tokens.get(start);
                Token b = tokens.get(Math.min(start + span, size - 1));
                String text = "<" + i + ">";
                description.append(
                        String.format(
                                "%n%s %d..%d %s", KINDS[kind], start, b.getTokenIndex(), text));
                switch (kind) {
                    case 0:
                        actualOps.add(r -> r.insertBefore(a, text));
                        expectedOps.add(r -> r.insertBefore(a, text));
                        break;
                    case 1:
                        actualOps.add(r -> r.insertAfter(a, text));
                        expectedOps.add(r -> r.insertAfter(a, text));
                        break;
                    case 2:
                        actualOps.add(r -> r.replace(a, b, text));
                        expectedOps.add(r -> r.replace(a, b, text));
                        break;
                    default:
                        actualOps.add(r -> r.delete(a, b));
                        expectedOps.add(r -> r.delete(a, b));
                }
            }
        }
    }

    @Test
    void orphanInsertsFoldedIntoLaterReplace() {
        CommonTokenStream tokens =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(CODE)));
        FlinkTokenStreamRewriter rewriter = new FlinkTokenStreamRewriter(tokens);

        rewriter.delete(tokens.get(5), tokens.get(6));
        rewriter.insertAfter(tokens.get(4), "A");
        rewriter.delete(tokens.get(6), tokens.get(7)); // merges with the first delete
        rewriter.insertAfter(tokens.get(4), "B");
        rewriter.replace(tokens.get(5), tokens.get(8), "R");

        assertThat(rewriter.getText())
                .isEqualTo(
                        "class A {BAR = 1; void f(int x) { if (x > 0) { a = x; } else { return; } } }");
    }

    @Test
    void eofInsertInsideMergedDelete() {
        CommonTokenStream tokens =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(CODE)));
        FlinkTokenStreamRewriter rewriter = new FlinkTokenStreamRewriter(tokens);
        int eof = tokens.size() - 1;

        rewriter.delete(tokens.get(eof - 3), tokens.get(eof));
        rewriter.insertBefore(tokens.get(eof), "E");
        rewriter.delete(tokens.get(eof - 5), tokens.get(eof - 2)); // merges with the first delete

        assertThat(rewriter.getText())
                .isEqualTo("class A { int a = 1; void f(int x) { if (x > 0) { a = x; } else { return; E");
    }

    @Test
    void emptyRangeRejected() {
        CommonTokenStream tokens =
                new CommonTokenStream(new JavaLexer(CharStreams.fromString(CODE)));
        FlinkTokenStreamRewriter rewriter = new FlinkTokenStreamRewriter(tokens);

        // stop == start - 1, as for an empty ParserRuleContext
        assertThatThrownBy(() -> rewriter.delete(tokens.get(5), tokens.get(4)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static IntStream chunks() {
        return IntStream.range(0, CHUNKS);
    }

    private static String apply(Supplier<String> text) {
        try {
            return text.get();
        } catch (IllegalArgumentException e) {
            return FAILED;
        } catch (Error e) {
            if (!isOneOpPerIndexError(e)) {
                throw e;
            }
            return FAILED;
        }
    }

    /** TokenStreamRewriter throws this instead of IllegalArgumentException in some cases. */
    private static boolean isOneOpPerIndexError(Error e) {
        return "should only be one op per index".equals(e.getMessage());
    }
}
