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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Enables to delete all existing data in a {@link DynamicTableSink} table using {@code TRUNCATE
 * TABLE} statement.
 *
 * <p>For {@code TRUNCATE TABLE} statement, if the corresponding {@link DynamicTableSink} have
 * implemented this interface, then the method {@link #executeTruncation()} will be invoked in
 * execution phase. Otherwise, Flink will throw exception directly.
 */
@PublicEvolving
public interface SupportsTruncate {

    /**
     * Execute truncating table.
     *
     * <p>Note: please remember to throw exception if the truncation hasn't been executed
     * successfully, otherwise it'll be still considered to haven been executed successfully by
     * Flink.
     */
    void executeTruncation();
}
