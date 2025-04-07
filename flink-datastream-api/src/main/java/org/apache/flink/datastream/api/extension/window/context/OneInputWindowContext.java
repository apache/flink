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

package org.apache.flink.datastream.api.extension.window.context;

import org.apache.flink.annotation.Experimental;

/**
 * This interface extends {@link WindowContext} and provides additional functionality for writing
 * and reading window data of one input window.
 *
 * @param <IN> Type of the input elements
 */
@Experimental
public interface OneInputWindowContext<IN> extends WindowContext {

    /**
     * Write records into the window's state.
     *
     * @param record The record to be written into the window's state.
     */
    void putRecord(IN record);

    /**
     * Read records from the window's state.
     *
     * @return Iterable of records, which could be null if the window is empty.
     */
    Iterable<IN> getAllRecords();
}
