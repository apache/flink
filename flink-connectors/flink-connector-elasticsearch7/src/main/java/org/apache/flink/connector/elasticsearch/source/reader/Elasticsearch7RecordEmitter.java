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

package org.apache.flink.connector.elasticsearch.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7SplitState;

/** The {@link RecordEmitter} implementation for both {@link Elasticsearch7SourceReader}. */
@PublicEvolving
public class Elasticsearch7RecordEmitter<T>
        implements RecordEmitter<Elasticsearch7Record<T>, T, Elasticsearch7SplitState> {

    @Override
    public void emitRecord(
            Elasticsearch7Record<T> element,
            SourceOutput<T> output,
            Elasticsearch7SplitState splitState)
            throws Exception {
        // Sink the record to source output.
        output.collect(element.getValue());
    }
}
