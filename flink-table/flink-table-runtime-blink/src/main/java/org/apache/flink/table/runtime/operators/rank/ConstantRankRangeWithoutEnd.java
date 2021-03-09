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

package org.apache.flink.table.runtime.operators.rank;

import java.util.List;

/** ConstantRankRangeWithoutEnd is a RankRange which not specify RankEnd. */
public class ConstantRankRangeWithoutEnd implements RankRange {

    private static final long serialVersionUID = -1944057111062598696L;

    private final long rankStart;

    public ConstantRankRangeWithoutEnd(long rankStart) {
        this.rankStart = rankStart;
    }

    @Override
    public String toString(List<String> inputFieldNames) {
        return toString();
    }

    @Override
    public String toString() {
        return "rankStart=" + rankStart;
    }
}
