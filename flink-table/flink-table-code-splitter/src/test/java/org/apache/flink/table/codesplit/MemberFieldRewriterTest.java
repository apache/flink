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

/** Tests for {@link MemberFieldRewriter}. */
class MemberFieldRewriterTest extends CodeRewriterTestBase<MemberFieldRewriter> {

    public MemberFieldRewriterTest() {
        super("member", code -> new MemberFieldRewriter(code, 3));
    }

    @Test
    void testRewriteMemberField() {
        runTest("TestRewriteMemberField");
    }

    @Test
    void testRewriteGenericType() {
        runTest("TestRewriteGenericType");
    }

    @Test
    void testNotRewriteFunctionParameter() {
        runTest("TestNotRewriteFunctionParameter");
    }

    @Test
    void testNotRewriteLocalVariable() {
        runTest("TestNotRewriteLocalVariable");
    }

    @Test
    void testNotRewriteMember() {
        runTest("TestNotRewriteMember");
    }

    @Test
    void testRewriteInnerClass() {
        runTest("TestRewriteInnerClass");
    }
}
