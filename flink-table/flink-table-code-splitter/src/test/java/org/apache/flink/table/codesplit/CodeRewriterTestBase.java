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

import org.apache.flink.util.FileUtils;

import java.io.File;
import java.util.function.Function;

import static org.apache.flink.table.codesplit.CodeSplitTestUtil.trimLines;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link CodeRewriter}. */
abstract class CodeRewriterTestBase<R extends CodeRewriter> {

    private final String resourceDir;
    private final Function<String, R> rewriterProvider;

    public CodeRewriterTestBase(String resourceDir, Function<String, R> rewriterProvider) {
        this.resourceDir = resourceDir;
        this.rewriterProvider = rewriterProvider;
    }

    R runTest(String filename) {
        try {
            String code =
                    FileUtils.readFileUtf8(
                            new File(
                                    CodeRewriterTestBase.class
                                            .getClassLoader()
                                            .getResource(
                                                    resourceDir + "/code/" + filename + ".java")
                                            .toURI()));
            String expected =
                    FileUtils.readFileUtf8(
                            new File(
                                    CodeRewriterTestBase.class
                                            .getClassLoader()
                                            .getResource(
                                                    resourceDir + "/expected/" + filename + ".java")
                                            .toURI()));

            R rewriter = rewriterProvider.apply(code);

            // Trying to mitigate any indentation issues between all sort of platforms by simply
            // trim every line of the "class". Before this change, code-splitter test could fail on
            // Windows machines while passing on Unix.
            expected = trimLines(expected);
            String actual = trimLines(rewriter.rewrite());

            assertThat(actual).isEqualTo(expected);
            return rewriter;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            // we reset the counter to ensure the variable names after rewrite are as expected
            CodeSplitUtil.getCounter().set(0L);
        }
    }
}
