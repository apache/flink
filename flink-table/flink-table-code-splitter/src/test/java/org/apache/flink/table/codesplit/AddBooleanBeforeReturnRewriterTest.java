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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AddBoolBeforeReturnRewriter}. */
class AddBooleanBeforeReturnRewriterTest extends CodeRewriterTestBase<AddBoolBeforeReturnRewriter> {

    public AddBooleanBeforeReturnRewriterTest() {
        super("add-boolean", code -> new AddBoolBeforeReturnRewriter(code, 50));
    }

    @Test
    void testAddBooleanBeforeReturn() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestAddBooleanBeforeReturn");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).hasSize(2);
        assertThat(result.get(0).get("fun1(int a)")).isEqualTo("fun1HasReturned$0");
        assertThat(result.get(0).get("fun2(String b)")).isEqualTo("fun2HasReturned$1");
    }

    @Test
    void testRewriteInnerClass() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestRewriteInnerClass");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        assertThat(result).hasSize(3);
        assertThat(result.get(0)).hasSize(1);
        assertThat(result.get(0).get("fun(int a)")).isEqualTo("funHasReturned$0");
        assertThat(result.get(1)).hasSize(1);
        assertThat(result.get(1).get("fun(int a)")).isEqualTo("funHasReturned$1");
        assertThat(result.get(2)).hasSize(1);
        assertThat(result.get(2).get("fun(int a)")).isEqualTo("funHasReturned$2");
    }

    @Test
    void testNotRewrite() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestNotRewrite");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEmpty();
    }

    @Test
    void testSkipAnonymousClassAndLambda() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestSkipAnonymousClassAndLambda");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        assertThat(result).hasSize(2);
        assertThat(result.get(0)).hasSize(1);
        assertThat(result.get(0).get("fun(String a)")).isEqualTo("funHasReturned$0");
        assertThat(result.get(1)).isEmpty();
    }
}
