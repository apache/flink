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

package org.apache.flink.datastream.api.context;

import org.apache.flink.annotation.Experimental;

/** This is responsibility for managing runtime information related to state of process function. */
@Experimental
public interface StateManager {
    /**
     * Get the key of current record.
     *
     * @return The key of current processed record.
     * @throws UnsupportedOperationException if the key can not be extracted for this function, for
     *     instance, get the key from a non-keyed partition stream.
     */
    <K> K getCurrentKey() throws UnsupportedOperationException;
}
