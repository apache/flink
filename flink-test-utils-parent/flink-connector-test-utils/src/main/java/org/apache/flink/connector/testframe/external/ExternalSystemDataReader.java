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

package org.apache.flink.connector.testframe.external;

import org.apache.flink.annotation.Experimental;

import java.time.Duration;
import java.util.List;

/**
 * A data reader for consuming records from an external system.
 *
 * @param <T> Type of the consuming record
 */
@Experimental
public interface ExternalSystemDataReader<T> extends AutoCloseable {

    /**
     * Poll a batch of records from external system.
     *
     * <p>Test cases will keep invoking this method until expected records have been polled, so it's
     * not necessary to fetch all records in one poll, but records should not be duplicated across
     * multiple invocations.
     *
     * @param timeout The maximum time to block
     */
    List<T> poll(Duration timeout);
}
