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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * A factory for creating source reader instances.
 *
 * @param <T> The type of the output elements.
 */
@Public
public interface SourceReaderFactory<T, SplitT extends SourceSplit> extends Serializable {
    /**
     * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
     * and does not have any state to resume.
     *
     * @param readerContext The {@link SourceReaderContext context} for the source reader.
     * @return A new SourceReader.
     * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
     *     thrown from this method cause task failure/recovery.
     */
    SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception;
}
