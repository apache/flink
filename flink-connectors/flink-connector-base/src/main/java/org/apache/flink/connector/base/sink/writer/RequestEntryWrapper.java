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

/**
 * A wrapper that contains a {@code RequestEntryT} ready to be written by the Sink Writer class,
 * along with the size of that entry as defined by the method {@code getSizeInBytes(RequestEntryT)}
 * of the {@code AsyncSinkWriter}.
 *
 * @param <RequestEntryT> Corresponds to the type parameter of the same name in {@code
 *     AsyncSinkWriter}
 */
@PublicEvolving
public class RequestEntryWrapper<RequestEntryT> {

    private final RequestEntryT requestEntry;
    private final long size;

    public RequestEntryWrapper(RequestEntryT requestEntry, long size) {
        this.requestEntry = requestEntry;
        this.size = size;
    }

    public RequestEntryT getRequestEntry() {
        return requestEntry;
    }

    public long getSize() {
        return size;
    }
}
