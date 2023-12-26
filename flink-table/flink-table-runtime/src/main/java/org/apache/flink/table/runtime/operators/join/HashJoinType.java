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

package org.apache.flink.table.runtime.operators.join;

/** Join type for hash table. */
public enum HashJoinType {
    INNER,
    BUILD_OUTER,
    PROBE_OUTER,
    FULL_OUTER,
    SEMI,
    ANTI,
    BUILD_LEFT_SEMI,
    BUILD_LEFT_ANTI;

    public boolean isBuildOuter() {
        return this.equals(BUILD_OUTER) || this.equals(FULL_OUTER);
    }

    public boolean isProbeOuter() {
        return this.equals(PROBE_OUTER) || this.equals(FULL_OUTER);
    }

    public boolean leftSemiOrAnti() {
        return this.equals(SEMI) || this.equals(ANTI);
    }

    public boolean buildLeftSemiOrAnti() {
        return this.equals(BUILD_LEFT_SEMI) || this.equals(BUILD_LEFT_ANTI);
    }

    public boolean needSetProbed() {
        return isBuildOuter() || buildLeftSemiOrAnti();
    }

    public static HashJoinType of(boolean leftIsBuild, boolean leftOuter, boolean rightOuter) {
        if (leftOuter && rightOuter) {
            return FULL_OUTER;
        } else if (leftOuter) {
            return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
        } else if (rightOuter) {
            return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
        } else {
            return INNER;
        }
    }

    public static HashJoinType of(
            boolean leftIsBuild,
            boolean leftOuter,
            boolean rightOuter,
            boolean isSemi,
            boolean isAnti) {
        if (leftOuter && rightOuter) {
            return FULL_OUTER;
        } else if (leftOuter) {
            return leftIsBuild ? BUILD_OUTER : PROBE_OUTER;
        } else if (rightOuter) {
            return leftIsBuild ? PROBE_OUTER : BUILD_OUTER;
        } else if (isSemi) {
            return leftIsBuild ? BUILD_LEFT_SEMI : SEMI;
        } else if (isAnti) {
            return leftIsBuild ? BUILD_LEFT_ANTI : ANTI;
        } else {
            return INNER;
        }
    }
}
