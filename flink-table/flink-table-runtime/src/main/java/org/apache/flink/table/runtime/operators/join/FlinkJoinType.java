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

/** Join type for join. */
public enum FlinkJoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    SEMI,
    ANTI;

    public boolean isOuter() {
        switch (this) {
            case LEFT:
            case RIGHT:
            case FULL:
                return true;
            default:
                return false;
        }
    }

    public boolean isLeftOuter() {
        switch (this) {
            case LEFT:
            case FULL:
                return true;
            default:
                return false;
        }
    }

    public boolean isRightOuter() {
        switch (this) {
            case RIGHT:
            case FULL:
                return true;
            default:
                return false;
        }
    }

    public boolean isSemiAnti() {
        switch (this) {
            case SEMI:
            case ANTI:
                return true;
            default:
                return false;
        }
    }

    @Override
    public String toString() {
        switch (this) {
            case INNER:
                return "InnerJoin";
            case LEFT:
                return "LeftOuterJoin";
            case RIGHT:
                return "RightOuterJoin";
            case FULL:
                return "FullOuterJoin";
            case SEMI:
                return "LeftSemiJoin";
            case ANTI:
                return "LeftAntiJoin";
            default:
                throw new IllegalArgumentException("Invalid join type: " + name());
        }
    }
}
