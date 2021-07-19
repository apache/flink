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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.connector.hbase.HBaseEvent;

import java.io.Serializable;

/**
 * The serialization interface that needs to be implemented for constructing an {@link HBaseSink}.
 *
 * <p>A minimal implementation can be seen in the following example.
 *
 * <pre>{@code
 * static class HBaseLongSerializer implements HBaseSinkSerializer<Long> {
 *     @Override
 *     public HBaseEvent serialize(Long event) {
 *         return HBaseEvent.putWith(                  // or deleteWith()
 *                 event.toString(),                   // rowId
 *                 "exampleColumnFamily",              // column family
 *                 "exampleQualifier",                 // qualifier
 *                 Bytes.toBytes(event.toString()));   // payload
 *     }
 * }
 * }</pre>
 */
@FunctionalInterface
public interface HBaseSinkSerializer<T> extends Serializable {
    HBaseEvent serialize(T event);
}
