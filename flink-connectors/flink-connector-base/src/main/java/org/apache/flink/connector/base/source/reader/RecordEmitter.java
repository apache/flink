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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * Emit a record to the downstream.
 *
 * @param <E> the type of the record emitted by the {@link SplitReader}
 * @param <T> the type of records that are eventually emitted to the {@link SourceOutput}.
 * @param <SplitStateT> the mutable type of split state.
 */
@PublicEvolving
public interface RecordEmitter<E, T, SplitStateT> {

    /**
     * Process and emit the records to the {@link SourceOutput}. A few recommendations to the
     * implementation are following:
     *
     * <ul>
     *   <li>The method maybe interrupted in the middle. In that case, the same set of records will
     *       be passed to the record emitter again later. The implementation needs to make sure it
     *       reades
     *   <li>
     * </ul>
     *
     * @param element The intermediate element read by the SplitReader.
     * @param output The output to which the final records are emit to.
     * @param splitState The state of the split.
     */
    void emitRecord(E element, SourceOutput<T> output, SplitStateT splitState) throws Exception;
}
