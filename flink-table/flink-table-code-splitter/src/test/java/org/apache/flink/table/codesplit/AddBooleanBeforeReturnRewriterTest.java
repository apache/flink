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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/** Tests for {@link AddBoolBeforeReturnRewriter}. */
public class AddBooleanBeforeReturnRewriterTest
        extends CodeRewriterTestBase<AddBoolBeforeReturnRewriter> {

    public AddBooleanBeforeReturnRewriterTest() {
        super("add-boolean", code -> new AddBoolBeforeReturnRewriter(code, 50));
    }

    @Test
    public void testAddBooleanBeforeReturn() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestAddBooleanBeforeReturn");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(2, result.get(0).size());
        Assert.assertEquals("fun1HasReturned$0", result.get(0).get("fun1(int a)"));
        Assert.assertEquals("fun2HasReturned$1", result.get(0).get("fun2(String b)"));
    }

    @Test
    public void testRewriteInnerClass() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestRewriteInnerClass");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals("funHasReturned$0", result.get(0).get("fun(int a)"));
        Assert.assertEquals(1, result.get(1).size());
        Assert.assertEquals("funHasReturned$1", result.get(1).get("fun(int a)"));
        Assert.assertEquals(1, result.get(2).size());
        Assert.assertEquals("funHasReturned$2", result.get(2).get("fun(int a)"));
    }

    @Test
    public void testNotRewrite() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestNotRewrite");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(0, result.get(0).size());
    }

    @Test
    public void testSkipAnonymousClassAndLambda() {
        AddBoolBeforeReturnRewriter rewriter = runTest("TestSkipAnonymousClassAndLambda");
        List<Map<String, String>> result = rewriter.getBoolVarNames();
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(1, result.get(0).size());
        Assert.assertEquals("funHasReturned$0", result.get(0).get("fun(String a)"));
        Assert.assertEquals(0, result.get(1).size());
    }
}
