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

import org.junit.jupiter.api.Test;

/** Tests for {@link BlockStatementRewriterTest}. */
class BlockStatementRewriterTest extends CodeRewriterTestBase<BlockStatementRewriter> {

    public BlockStatementRewriterTest() {
        super("block", code -> new BlockStatementRewriter(code, 20));
    }

    @Test
    void testIfInsideWhileLoopRewrite() {
        runTest("TestIfInsideWhileLoopRewrite");
    }

    @Test
    void testWhileLoopInsideIfRewrite() {
        runTest("TestWhileLoopInsideIfRewrite");
    }

    @Test
    void testWhileLoopRewrite() {
        runTest("TestWhileLoopRewrite");
    }

    @Test
    void testIfStatementRewrite() {
        runTest("TestIfStatementRewrite");
    }

    @Test
    void testIfStatementRewrite1() {
        runTest("TestIfStatementRewrite1");
    }

    @Test
    void testIfStatementRewrite2() {
        runTest("TestIfStatementRewrite2");
    }

    @Test
    void testIfStatementRewrite3() {
        runTest("TestIfStatementRewrite3");
    }

    @Test
    void testIfMultipleSingleLineStatementRewrite() {
        runTest("TestIfMultipleSingleLineStatementRewrite");
    }

    @Test
    void testNotRewriteIfStatementInFunctionWithReturnValue() {
        runTest("TestNotRewriteIfStatementInFunctionWithReturnValue");
    }

    @Test
    void testRewriteInnerClass() {
        runTest("TestRewriteInnerClass");
    }

    @Test
    void testRewriteTwoStatements() {
        runTest("TestRewriteTwoStatements");
    }

    /**
     * Check whether the given and expected classes are actually a valid Java code -> it compiles.
     * If this test fails on "expected" files, it probably means that code split logic is invalid
     * and an issue was missed when preparing test files.
     */
    @Test
    void shouldCompileGivenAndExpectedCode() throws Exception {
        CodeSplitTestUtil.tryCompile("block/code/");
        CodeSplitTestUtil.tryCompile("block/expected/");
    }
}
