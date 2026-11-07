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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkTokenStreamRewriter}. */
class FlinkTokenStreamRewriterTest {

    private static final String CODE =
            "class A { int a = 1; void f(int x) { if (x > 0) { a = x; } else { return; } } }";

    private static final String[] KINDS = {"insertBefore", "insertAfter", "replace", "delete"};

    private static final int SEEDS = 100_000;
    private static final int CHUNKS = 8;

    @ParameterizedTest
    @MethodSource("chunks")
    @Execution(ExecutionMode.CONCURRENT)
    void sameOutputAsTokenStreamRewriter(int chunk) {
        for (long seed = (long) chunk * SEEDS / CHUNKS;
                seed < (long) (chunk + 1) * SEEDS / CHUNKS;
                seed++) {
            Random random = new Random(seed);
            CommonTokenStream tokens =
                    new CommonTokenStream(new JavaLexer(CharStreams.fromString(CODE)));
            // fills the token stream, so it goes before tokens.size()
            FlinkTokenStreamRewriter actual = new FlinkTokenStreamRewriter(tokens);
            TokenStreamRewriter expected = new TokenStreamRewriter(tokens);
            int size = tokens.size();

            List<Runnable> actualOps = new ArrayList<>();
            List<Runnable> expectedOps = new ArrayList<>();
            StringBuilder description = new StringBuilder();
            for (int i = random.nextInt(8); i >= 0; i--) {
                int start = random.nextInt(size - 1);
                Token a = tokens.get(start);
                Token b = tokens.get(Math.min(start + random.nextInt(4), size - 2));
                String text = "<" + i + ">";
                int kind = random.nextInt(4);
                description.append(
                        String.format(
                                "%n%s %d..%d %s", KINDS[kind], start, b.getTokenIndex(), text));
                switch (kind) {
                    case 0:
                        actualOps.add(() -> actual.insertBefore(a, text));
                        expectedOps.add(() -> expected.insertBefore(a, text));
                        break;
                    case 1:
                        actualOps.add(() -> actual.insertAfter(a, text));
                        expectedOps.add(() -> expected.insertAfter(a, text));
                        break;
                    case 2:
                        actualOps.add(() -> actual.replace(a, b, text));
                        expectedOps.add(() -> expected.replace(a, b, text));
                        break;
                    default:
                        actualOps.add(() -> actual.delete(a, b));
                        expectedOps.add(() -> expected.delete(a, b));
                }
            }
            int from = random.nextInt(size);
            Interval interval = Interval.of(from, from + random.nextInt(size - from));

            // a single getText call, TokenStreamRewriter#getText changes its state
            assertThat(apply(actualOps, () -> actual.getText(interval)))
                    .as("seed %d%s", seed, description)
                    .isEqualTo(apply(expectedOps, () -> expected.getText(interval)));
        }
    }

    private static IntStream chunks() {
        return IntStream.range(0, CHUNKS);
    }

    private static String apply(List<Runnable> ops, Supplier<String> text) {
        try {
            ops.forEach(Runnable::run);
            return text.get();
        } catch (IllegalArgumentException e) {
            return "failed";
        } catch (Error e) {
            if (!isOneOpPerIndexError(e)) {
                throw e;
            }
            return "failed";
        }
    }

    /** TokenStreamRewriter throws this instead of IllegalArgumentException in some cases. */
    private static boolean isOneOpPerIndexError(Error e) {
        return "should only be one op per index".equals(e.getMessage());
    }
}
