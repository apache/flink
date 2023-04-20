/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.Serializable;

/**
 * This interface specifies the mapping between elements of a stream to request entries that can be
 * sent to the destination. The mapping is provided by the end-user of a sink, not the sink creator.
 *
 * <p>The request entries contain all relevant information required to create and sent the actual
 * request. Eg, for Kinesis Data Streams, the request entry includes the payload and the partition
 * key.
 */
@PublicEvolving
public interface ElementConverter<InputT, RequestEntryT> extends Serializable {
    RequestEntryT apply(InputT element, SinkWriter.Context context);

    default void open(Sink.InitContext context) {
        // No-op default implementation
    }
}
