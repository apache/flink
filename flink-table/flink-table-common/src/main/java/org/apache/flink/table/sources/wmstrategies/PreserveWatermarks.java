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

package org.apache.flink.table.sources.wmstrategies;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.Rowtime;

import java.util.HashMap;
import java.util.Map;

/** A strategy which indicates the watermarks should be preserved from the underlying datastream. */
@PublicEvolving
public final class PreserveWatermarks extends WatermarkStrategy {

    private static final long serialVersionUID = 1L;

    public static final PreserveWatermarks INSTANCE = new PreserveWatermarks();

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PreserveWatermarks;
    }

    @Override
    public int hashCode() {
        return PreserveWatermarks.class.hashCode();
    }

    @Override
    public Map<String, String> toProperties() {
        Map<String, String> map = new HashMap<>();
        map.put(Rowtime.ROWTIME_WATERMARKS_TYPE, Rowtime.ROWTIME_WATERMARKS_TYPE_VALUE_FROM_SOURCE);
        return map;
    }
}
