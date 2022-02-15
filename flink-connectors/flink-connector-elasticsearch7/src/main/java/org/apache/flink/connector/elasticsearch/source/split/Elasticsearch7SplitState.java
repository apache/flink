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

/**
 * This class extends {@link Elasticsearch7Split} for potential tracking of additional (meta-)data.
 */
@Internal
public class Elasticsearch7SplitState extends Elasticsearch7Split {
    public Elasticsearch7SplitState(Elasticsearch7Split elasticsearchSplit) {
        super(elasticsearchSplit.getPitId(), elasticsearchSplit.getSliceId());
    }

    public Elasticsearch7Split toElasticsearchSplit() {
        return new Elasticsearch7Split(this.getPitId(), this.getSliceId());
    }
}
