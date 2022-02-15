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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The state of {@link Elasticsearch7SourceEnumerator}. */
@Internal
public class Elasticsearch7SourceEnumState {

    private final Set<Elasticsearch7Split> assignedSplits;

    Elasticsearch7SourceEnumState(Set<Elasticsearch7Split> assignedSplits) {
        this.assignedSplits = checkNotNull(assignedSplits);
    }

    public Set<Elasticsearch7Split> getAssignedSplits() {
        return assignedSplits;
    }

    public static Elasticsearch7SourceEnumState fromCollectionSnapshot(
            final Collection<Elasticsearch7Split> splits) {
        return new Elasticsearch7SourceEnumState(new HashSet<>(splits));
    }
}
