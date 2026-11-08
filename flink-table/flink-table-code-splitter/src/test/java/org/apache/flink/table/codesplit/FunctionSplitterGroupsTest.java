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

import org.codehaus.janino.SimpleCompiler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FunctionSplitter} grouping split calls that do not fit into one method. */
class FunctionSplitterGroupsTest extends CodeRewriterTestBase<FunctionSplitter> {

    private static final int MAX_METHOD_LENGTH = 60;
    private static final int STATEMENTS = 16;

    // a split method whose body only calls other split methods
    private static final String GROUP_METHOD =
            "void myFun_split\\d+\\(int\\[\\] a, int\\[\\] b\\) \\{\\s*myFun_split\\d+\\(a, b\\);";

    FunctionSplitterGroupsTest() {
        super("function", code -> new FunctionSplitter(code, MAX_METHOD_LENGTH));
    }

    @AfterEach
    void resetCounter() {
        CodeSplitUtil.getCounter().set(0L);
    }

    @Test
    void testGroupedSplitCalls() {
        // 16 statements: 5 split methods, whose calls are grouped into 2 methods and 1 call
        runTest("TestSplitFunctionGroups");
    }

    @Test
    void testGroupedSplitCallsWithReturn() {
        // the early return is checked after every call that leads to it, at every level
        runTest("TestSplitFunctionGroupsReturn");
    }

    @Test
    void testSplitCallsAreGrouped() throws Exception {
        final String code = function(false);
        final String rewritten = new FunctionSplitter(code, MAX_METHOD_LENGTH).rewrite();

        assertThat(rewritten).containsPattern(GROUP_METHOD);
        assertSameResult(code, rewritten, new int[STATEMENTS]);
    }

    @Test
    void testEarlyReturnInGroupedCalls() throws Exception {
        final String code = function(true);
        final String rewritten = new FunctionSplitter(code, MAX_METHOD_LENGTH).rewrite();

        assertThat(rewritten).containsPattern(GROUP_METHOD);
        // returns early
        final int[] returning = new int[STATEMENTS];
        returning[STATEMENTS / 2] = 1;
        assertSameResult(code, rewritten, returning);
        // runs to the end
        assertSameResult(code, rewritten, new int[STATEMENTS]);
    }

    private static String function(boolean withReturn) {
        final StringBuilder body = new StringBuilder();
        for (int i = 0; i < STATEMENTS; i++) {
            if (withReturn && i == STATEMENTS / 2) {
                body.append("if (a[").append(i).append("] > 0) { a[0] = 100; return; }\n");
            }
            body.append("a[").append(i).append("] += b[").append(i).append("];\n");
        }
        return "public class TestSplitGroups {\n"
                + "public void myFun(int[] a, int[] b) {\n"
                + body
                + "}\n"
                + "}\n";
    }

    private static void assertSameResult(String code, String rewritten, int[] a) throws Exception {
        final int[] b = new int[STATEMENTS];
        Arrays.setAll(b, i -> i + 1);
        assertThat(run(rewritten, a.clone(), b)).isEqualTo(run(code, a.clone(), b));
    }

    private static int[] run(String code, int[] a, int[] b) throws Exception {
        final SimpleCompiler compiler = new SimpleCompiler();
        compiler.cook(code);
        final Class<?> clazz = compiler.getClassLoader().loadClass("TestSplitGroups");
        final Method myFun = clazz.getMethod("myFun", int[].class, int[].class);
        myFun.invoke(clazz.getConstructor().newInstance(), a, b);
        return a;
    }
}
