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

import org.apache.flink.annotation.Internal;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Rewrite generated java code so that the length of each method becomes smaller and can be
 * compiled.
 */
@Internal
public class JavaCodeSplitter {

    public static String split(String code, int maxMethodLength, int maxClassMemberCount) {
        try {
            if (code.length() <= maxMethodLength) {
                return code;
            }
            return splitImpl(code, maxMethodLength, maxClassMemberCount);
        } catch (Throwable t) {
            System.out.println(code);
            throw new RuntimeException(
                    "JavaCodeSplitter failed. This is a bug. Please file an issue.", t);
        }
    }

    private static String splitImpl(String code, int maxMethodLength, int maxClassMemberCount) {
        checkArgument(code != null && !code.isEmpty(), "code cannot be empty");
        checkArgument(maxMethodLength > 0);
        checkArgument(maxClassMemberCount > 0);

        String returnValueRewrittenCode = new ReturnValueRewriter(code, maxMethodLength).rewrite();
        return Optional.ofNullable(
                        new DeclarationRewriter(returnValueRewrittenCode, maxMethodLength)
                                .rewrite())
                .map(text -> new IfStatementRewriter(text, maxMethodLength).rewrite())
                .map(text -> new FunctionSplitter(text, maxMethodLength).rewrite())
                .map(text -> new MemberFieldRewriter(text, maxClassMemberCount).rewrite())
                .orElse(code);
    }
}
