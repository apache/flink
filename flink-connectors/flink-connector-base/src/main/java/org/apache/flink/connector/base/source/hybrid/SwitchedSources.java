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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/** Sources that participated in switching with cached serializers. */
class SwitchedSources {
    private final Map<Integer, Source> sources = new HashMap<>();
    private final Map<Integer, SimpleVersionedSerializer<SourceSplit>> cachedSerializers =
            new HashMap<>();

    public Source sourceOf(int sourceIndex) {
        return Preconditions.checkNotNull(
                sources.get(sourceIndex), "Source for index=%s not available", sourceIndex);
    }

    public SimpleVersionedSerializer<SourceSplit> serializerOf(int sourceIndex) {
        return cachedSerializers.computeIfAbsent(
                sourceIndex, (k -> sourceOf(k).getSplitSerializer()));
    }

    public void put(int sourceIndex, Source source) {
        sources.put(sourceIndex, Preconditions.checkNotNull(source));
    }
}
