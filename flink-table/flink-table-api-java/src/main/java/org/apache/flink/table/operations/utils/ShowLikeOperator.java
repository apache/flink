/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.SqlLikeUtils;

import javax.annotation.Nullable;

import java.util.Objects;

/** Like operator for SHOW operations. */
@Internal
public class ShowLikeOperator {
    private final LikeType likeType;
    private final String likePattern;

    private ShowLikeOperator(LikeType likeType, String likePattern) {
        this.likeType = likeType;
        this.likePattern = likePattern;
    }

    public static ShowLikeOperator of(LikeType likeType, String likePattern) {
        return likeType == null ? null : new ShowLikeOperator(likeType, likePattern);
    }

    public static ShowLikeOperator of(boolean withLike, boolean isNotLike, String likePattern) {
        return of(withLike, false, isNotLike, likePattern);
    }

    public static ShowLikeOperator of(
            boolean withLike, boolean isILike, boolean isNotLike, String likePattern) {
        if (!withLike) {
            return null;
        }
        final LikeType likeType = LikeType.of(isILike, isNotLike);
        return new ShowLikeOperator(likeType, likePattern);
    }

    public static boolean likeFilter(String row, @Nullable ShowLikeOperator operator) {
        if (operator == null) {
            return true;
        }
        final boolean notLike = operator.likeType.isNot();
        final boolean isILike = operator.likeType.isILike();
        final boolean likeMatch =
                isILike
                        ? SqlLikeUtils.ilike(row, operator.likePattern, "\\")
                        : SqlLikeUtils.like(row, operator.likePattern, "\\");
        return notLike != likeMatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShowLikeOperator likeOp = (ShowLikeOperator) o;
        return likeType == likeOp.likeType && Objects.equals(likePattern, likeOp.likePattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(likeType, likePattern);
    }

    @Override
    public String toString() {
        return asSummaryString();
    }

    public String asSummaryString() {
        return likeType.asSummaryString() + " '" + likePattern + "'";
    }
}
