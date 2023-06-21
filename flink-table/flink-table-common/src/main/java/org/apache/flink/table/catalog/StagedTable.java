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
import org.apache.flink.core.execution.JobStatusHook;

import java.io.Serializable;

/**
 * A {@link StagedTable} for atomic semantics using a two-phase commit protocol, combined with
 * {@link JobStatusHook} for atomic CTAS. {@link StagedTable} will be a member variable of
 * CtasJobStatusHook and can be serialized;
 *
 * <p>CtasJobStatusHook#onCreated will call the begin method of StagedTable;
 * CtasJobStatusHook#onFinished will call the commit method of StagedTable;
 * CtasJobStatusHook#onFailed and CtasJobStatusHook#onCanceled will call the abort method of
 * StagedTable;
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
     * This method will be called when the job is failed or canceled. Similar to what it means to
     * rollback the transaction in a relational database; In Flink's atomic CTAS scenario, it is
     * used to do some data cleaning; For example, delete the data in tmp directory, delete the
     * temporary data in the underlying storage service, or even call the rollback transaction API
     * of the underlying service, etc.
     */
    void abort();
}
