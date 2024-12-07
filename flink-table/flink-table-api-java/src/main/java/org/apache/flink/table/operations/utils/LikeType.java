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

import java.util.Locale;

/** Like types enums. */
@Internal
public enum LikeType {
    /** case-sensitive to match a pattern. */
    LIKE("LIKE"),
    /** case-insensitive to match a pattern. */
    ILIKE("ILIKE"),
    NOT_LIKE("NOT LIKE"),
    NOT_ILIKE("NOT ILIKE");

    private final String name;

    LikeType(String name) {
        this.name = name;
    }

    public static LikeType of(String type) {
        return LikeType.valueOf(type.toUpperCase(Locale.ROOT));
    }

    public static LikeType of(String type, boolean notLike) {
        if (type == null) {
            return null;
        }
        LikeType likeType = LikeType.valueOf(type.toUpperCase(Locale.ROOT));
        if (notLike) {
            if (likeType == LIKE) {
                return NOT_LIKE;
            }
            return NOT_ILIKE;
        }
        return likeType;
    }

    public boolean isNot() {
        return this == NOT_LIKE || this == NOT_ILIKE;
    }

    public boolean isILike() {
        return this == ILIKE || this == NOT_ILIKE;
    }

    public static LikeType of(boolean isILike, boolean notLike) {
        if (isILike) {
            return notLike ? NOT_ILIKE : ILIKE;
        } else {
            return notLike ? NOT_LIKE : LIKE;
        }
    }

    public String asSummaryString() {
        return name;
    }
}
