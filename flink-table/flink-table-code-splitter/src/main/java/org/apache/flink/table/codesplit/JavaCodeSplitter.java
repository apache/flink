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
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava33.com.google.common.util.concurrent.ExecutionError;
import org.apache.flink.shaded.guava33.com.google.common.util.concurrent.UncheckedExecutionException;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Rewrite generated java code so that the length of each method becomes smaller and can be
 * compiled.
 */
@Internal
public class JavaCodeSplitter {

    // split() is a pure function of its inputs; cache results so identical code isn't re-split.
    private static final Cache<SplitKey, String> SPLIT_CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterAccess(Duration.ofMinutes(5))
                    .maximumSize(300)
                    .softValues()
                    .build();

    public static String split(String code, int maxMethodLength, int maxClassMemberCount) {
        checkArgument(code != null && !code.isEmpty(), "code cannot be empty");
        checkArgument(maxMethodLength > 0, "maxMethodLength must be greater than 0");
        checkArgument(maxClassMemberCount > 0, "maxClassMemberCount must be greater than 0");

        if (code.length() <= maxMethodLength) {
            return code;
        }

        SplitKey key = new SplitKey(code, maxMethodLength, maxClassMemberCount);
        try {
            return SPLIT_CACHE.get(
                    key, () -> splitImpl(code, maxMethodLength, maxClassMemberCount));
        } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new RuntimeException(
                    "JavaCodeSplitter failed. This is a bug. Please file an issue.", cause);
        }
    }

    @VisibleForTesting
    static String splitImpl(String code, int maxMethodLength, int maxClassMemberCount) {

        // reset counter so identical input yields identical output
        CodeSplitUtil.reset();

        String returnValueRewrittenCode = new ReturnValueRewriter(code, maxMethodLength).rewrite();
        return Optional.ofNullable(
                        new DeclarationRewriter(returnValueRewrittenCode, maxMethodLength)
                                .rewrite())
                .map(text -> new BlockStatementRewriter(text, maxMethodLength).rewrite())
                .map(text -> new FunctionSplitter(text, maxMethodLength).rewrite())
                .map(text -> new MemberFieldRewriter(text, maxClassMemberCount).rewrite())
                .orElse(code);
    }

    private static final class SplitKey {
        private final String code;
        private final int maxMethodLength;
        private final int maxClassMemberCount;

        SplitKey(String code, int maxMethodLength, int maxClassMemberCount) {
            this.code = code;
            this.maxMethodLength = maxMethodLength;
            this.maxClassMemberCount = maxClassMemberCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SplitKey)) {
                return false;
            }
            SplitKey that = (SplitKey) o;
            return maxMethodLength == that.maxMethodLength
                    && maxClassMemberCount == that.maxClassMemberCount
                    && Objects.equals(code, that.code);
        }

        @Override
        public int hashCode() {
            return Objects.hash(code, maxMethodLength, maxClassMemberCount);
        }
    }
}
