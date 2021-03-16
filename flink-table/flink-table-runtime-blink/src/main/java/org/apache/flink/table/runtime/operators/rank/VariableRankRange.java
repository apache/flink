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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

/** changing rank limit depends on input. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("Variable")
public class VariableRankRange implements RankRange {

    public static final String FIELD_NAME_END_INDEX = "endIndex";

    private static final long serialVersionUID = 5579785886506433955L;

    @JsonProperty(FIELD_NAME_END_INDEX)
    private final int rankEndIndex;

    @JsonCreator
    public VariableRankRange(@JsonProperty(FIELD_NAME_END_INDEX) int rankEndIndex) {
        this.rankEndIndex = rankEndIndex;
    }

    @JsonIgnore
    public int getRankEndIndex() {
        return rankEndIndex;
    }

    @Override
    public String toString(List<String> inputFieldNames) {
        return "rankEnd=" + inputFieldNames.get(rankEndIndex);
    }

    @Override
    public String toString() {
        return "rankEnd=$" + rankEndIndex;
    }
}
