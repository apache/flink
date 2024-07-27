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
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/** Utils to create the DataStream V2 supported {@link Source}. */
@Experimental
public final class DataStreamV2SourceUtils {
    /**
     * Wrap a FLIP-27 based source to a DataStream V2 supported source.
     *
     * @param source The FLIP-27 based source to wrap.
     * @return The DataStream V2 supported source.
     */
    public static <T> Source<T> wrapSource(
            org.apache.flink.api.connector.source.Source<T, ?, ?> source) {
        return new WrappedSource<>(source);
    }

    /**
     * Creates a source that contains the given elements.The type of the data stream is that of the
     * elements in the collection.
     *
     * @param data The collection of elements to create the source from.
     * @param <T> The generic type of the returned data stream.
     * @return The source representing the given collection
     */
    public static <T> Source<T> fromData(Collection<T> data) {
        Preconditions.checkNotNull(data, "Collection must not be null");
        return new FromDataSource<>(data);
    }
}
