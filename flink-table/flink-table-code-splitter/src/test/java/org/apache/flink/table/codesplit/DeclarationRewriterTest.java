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

import org.junit.Test;

/** Tests for {@link DeclarationRewriter}. */
public class DeclarationRewriterTest extends CodeRewriterTestBase<DeclarationRewriter> {

    public DeclarationRewriterTest() {
        super("declaration", code -> new DeclarationRewriter(code, 20));
    }

    @Test
    public void testRewriteLocalVariable() {
        runTest("TestRewriteLocalVariable");
    }

    @Test
    public void testNotRewriteLocalVariableInFunctionWithReturnValue() {
        runTest("TestNotRewriteLocalVariableInFunctionWithReturnValue");
    }

    @Test
    public void testRewriteLocalVariableInForLoop() {
        runTest("TestRewriteLocalVariableInForLoop1");
        runTest("TestRewriteLocalVariableInForLoop2");
    }

    @Test
    public void testLocalVariableWithSameName() {
        runTest("TestLocalVariableWithSameName");
    }

    @Test
    public void testRewriteInnerClass() {
        runTest("TestRewriteInnerClass");
    }

    @Test
    public void testLocalVariableAndMemberVariableWithSameName() {
        runTest("TestLocalVariableAndMemberVariableWithSameName");
    }
}
