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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceEvent;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A source event to transport the {@link DynamicFilteringData} for dynamic filtering purpose. The
 * event is sent by the DynamicFilteringDataCollector to the enumerator of a source that supports
 * dynamic filtering, via DynamicFilteringDataCollectorCoordinator and SourceCoordinator.
 */
@PublicEvolving
public class DynamicFilteringEvent implements SourceEvent {
    private final DynamicFilteringData data;

    public DynamicFilteringEvent(DynamicFilteringData data) {
        this.data = checkNotNull(data);
    }

    public DynamicFilteringData getData() {
        return data;
    }

    @Override
    public String toString() {
        return "DynamicFilteringEvent{" + "data=" + data + '}';
    }
}
