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

package org.apache.flink.api.connector.dsv2;

import org.apache.flink.annotation.Experimental;

/** Utils to create the DataStream V2 supported {@link Sink}. */
@Experimental
public class DataStreamV2SinkUtils {
    /**
     * Wrap a sink-v2 based sink to a DataStream V2 supported sink.
     *
     * @param sink The sink-v2 based sink to wrap.
     * @return The DataStream V2 supported sink.
     */
    public static <T> Sink<T> wrapSink(org.apache.flink.api.connector.sink2.Sink<T> sink) {
        return new WrappedSink<>(sink);
    }
}
