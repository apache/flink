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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsStaging;

import java.io.Serializable;

/**
 * The {@link StagedTable} is designed to implement Flink's atomic semantic for CTAS(CREATE TABLE AS
 * SELECT) and RTAS([CREATE OR] REPLACE TABLE AS SELECT) statement using a two-phase commit
 * protocol. The {@link StagedTable} is supposed to be returned via method {@link
 * SupportsStaging#applyStaging} by the {@link DynamicTableSink} which implements the {@link
 * SupportsStaging} interface.
 *
 * <p>When the Flink job for writing to a {@link DynamicTableSink} with atomic semantic supporting
 * is CREATED, the {@link StagedTable#begin()} will be called; when the Flink job is FINISHED, the
 * {@link StagedTable#commit()} will be called; when the Flink job is FAILED or CANCELED, the {@link
 * StagedTable#abort()} will be called;
 *
 * <p>See more in {@link SupportsStaging}.
 */
@PublicEvolving
public interface StagedTable extends Serializable {

    /**
     * This method will be called when the job is started. In Flink's atomic CTAS/RTAS scenario, it
     * is expected to do initialization work; For example, initializing the client of the underlying
     * service, the tmp path of the underlying storage, or even call the start transaction API of
     * the underlying service, etc.
     */
    void begin();

    /**
     * This method will be called when the job succeeds. In Flink's atomic CTAS/RTAS scenario, it is
     * expected to do some commit work. For example, moving the underlying data to the target
     * directory to make it visible, writing buffer data to the underlying storage service, or even
     * call the commit transaction API of the underlying service, etc.
     */
    void commit();

    /**
     * This method will be called when the job is failed or is canceled. In Flink's atomic CTAS/RTAS
     * scenario, it is expected to do some cleaning work for writing; For example, delete the data
     * in the tmp directory, delete the temporary data in the underlying storage service, or even
     * call the rollback transaction API of the underlying service, etc.
     */
    void abort();
}
