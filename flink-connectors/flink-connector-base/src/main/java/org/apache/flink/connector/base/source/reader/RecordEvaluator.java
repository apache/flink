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

import java.io.Serializable;

/**
 * An interface that evaluates whether a de-serialized record should trigger certain control-flow
 * operations (e.g. end of stream).
 */
@PublicEvolving
@FunctionalInterface
public interface RecordEvaluator<T> extends Serializable {
    /**
     * Determines whether a record should trigger the end of stream for its split. The given record
     * wouldn't be emitted from the source if the returned result is true.
     *
     * @param record a de-serialized record from the split.
     * @return a boolean indicating whether the split has reached end of stream.
     */
    boolean isEndOfStream(T record);
}
