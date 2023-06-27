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
 * A {@link StagedTable} for atomic semantics using a two-phase commit protocol. If {@link
 * DynamicTableSink} implements the {@link SupportsStaging} interface, it can return a {@link
 * StagedTable} object via the `applyStaging` method.
 *
 * <p>when flink job is CREATED, the begin method of StagedTable will be called; when flink job is
 * FINISHED, the commit method of StagedTable will be called; when flink job is FAILED or CANCELED,
 * the abort method of StagedTable will be called;
 */
@PublicEvolving
public interface StagedTable extends Serializable {

    /**
     * This method will be called when the job is started. Similar to what it means to open a
     * transaction in a relational database; In Flink's atomic CTAS scenario, it is used to do some
     * initialization work; For example, initializing the client of the underlying service, the tmp
     * path of the underlying storage, or even call the start transaction API of the underlying
     * service, etc.
     */
    void begin();

    /**
     * This method will be called when the job is succeeds. Similar to what it means to commit the
     * transaction in a relational database; In Flink's atomic CTAS scenario, it is used to do some
     * data visibility related work; For example, moving the underlying data to the target
     * directory, writing buffer data to the underlying storage service, or even call the commit
     * transaction API of the underlying service, etc.
     */
    void commit();

    /**
     * This method will be called when the job is failed or canceled. In Flink's atomic CTAS
     * scenario, it is expected to do some cleaning work for a writing; For example, delete the data
     * in tmp directory, delete the temporary data in the underlying storage service, or even call
     * the rollback transaction API of the underlying service, etc.
     */
    void abort();
}
