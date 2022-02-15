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

package org.apache.flink.connector.elasticsearch.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/**
 * The {@link SourceSplit} for the ElasticsearchSource. Each split contains the pit (point in time)
 * which was initialized in the {@code Elasticsearch7SourceEnumerator}. The pit is necessary to
 * allow consistent reads from Elasticsearch. To read a large number of documents a search can be
 * split into multiple slices to consume them independently. The number of slices is user-defined
 * and matches the total number of splits. Elasticsearch says the following about tuning the number
 * of slices:
 *
 * <p>If the number of slices is bigger than the number of shards the slice filter is very slow on
 * the first calls, it has a complexity of O(N) and a memory cost equals to N bits per slice where N
 * is the total number of documents in the shard. After few calls the filter should be cached and
 * subsequent calls should be faster but you should limit the number of sliced query you perform in
 * parallel to avoid the memory explosion.
 */
@PublicEvolving
public class Elasticsearch7Split implements SourceSplit {
    private final String pitId;
    private final int sliceId;

    public Elasticsearch7Split(String pitId, int sliceId) {
        this.pitId = pitId;
        this.sliceId = sliceId;
    }

    public String getPitId() {
        return pitId;
    }

    public int getSliceId() {
        return sliceId;
    }

    @Override
    public String splitId() {
        return pitId + "-" + sliceId;
    }

    @Override
    public String toString() {
        return String.format("[PitId: %s, SliceId: %d]", pitId, sliceId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Elasticsearch7Split that = (Elasticsearch7Split) o;
        return sliceId == that.sliceId && Objects.equals(pitId, that.pitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pitId, sliceId);
    }
}
