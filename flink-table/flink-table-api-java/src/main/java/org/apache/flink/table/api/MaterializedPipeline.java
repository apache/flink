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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StartMode;

/**
 * A pending declaration of one materialized table, created by {@link Table#materializeInto(String,
 * MaterializedTableDescriptor)}.
 */
@PublicEvolving
public interface MaterializedPipeline extends Explainable<MaterializedPipeline> {

    /**
     * The fully qualified identifier this declaration will create or alter.
     *
     * <p>Resolved against the current catalog and database at {@link Table#materializeInto}.
     */
    ObjectIdentifier getIdentifier();

    /**
     * Submits this declaration with the semantics of {@code CREATE OR ALTER MATERIALIZED TABLE}. It
     * creates the materialized table, or alters it if it exists. The methods of {@link
     * MaterializedTableDescriptor.Builder} describe what an existing materialized table keeps.
     *
     * <p>If the continuous refresh job of an existing materialized table is running, the job is
     * stopped with a savepoint and a new refresh job is started without restoring that savepoint,
     * so its streaming state starts empty. This also happens if the declaration is unchanged. A
     * suspended materialized table stays suspended, and a later resume starts its refresh job
     * without state.
     *
     * <p>Returns once the materialized table is created or altered and, if a refresh job is
     * started, once that job has been submitted.
     *
     * <p>Equivalent to {@code execute(CreateMode.createOrAlter())}.
     *
     * @return A handle on the materialized table.
     * @throws ValidationException if a declared {@link RefreshStrategy} differs from the refresh
     *     mode of the existing materialized table.
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp, or has an offset that is not positive.
     */
    MaterializedTable execute();

    /**
     * Like {@link #execute()}, with the given start mode in place of the descriptor's. Equivalent
     * to {@code execute(CreateMode.createOrAlter(), startMode)}.
     *
     * @param startMode The start mode of the materialized table.
     * @return A handle on the materialized table.
     * @throws ValidationException if a declared {@link RefreshStrategy} differs from the refresh
     *     mode of the existing materialized table.
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp, or has an offset that is not positive.
     */
    MaterializedTable execute(StartMode startMode);

    /**
     * Submits this declaration in the given mode. With {@link CreateMode#createOrAlter()}, this is
     * {@link #execute()}. With {@link CreateMode#failIfExists()}, it fails if a table or view
     * exists at the path, which is then left unchanged.
     *
     * @param mode What to do if an object exists at the path.
     * @return A handle on the materialized table.
     * @throws ValidationException if a declared {@link RefreshStrategy} differs from the refresh
     *     mode of the existing materialized table.
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp, or has an offset that is not positive.
     */
    MaterializedTable execute(CreateMode mode);

    /**
     * Like {@link #execute(CreateMode)}, with the given start mode in place of the descriptor's.
     *
     * @param mode What to do if an object exists at the path.
     * @param startMode The start mode of the materialized table.
     * @return A handle on the materialized table.
     * @throws ValidationException if a declared {@link RefreshStrategy} differs from the refresh
     *     mode of the existing materialized table.
     * @throws ValidationException if the start mode is {@code FROM_TIMESTAMP} or {@code
     *     RESUME_OR_FROM_TIMESTAMP} without a timestamp, or has an offset that is not positive.
     */
    MaterializedTable execute(CreateMode mode, StartMode startMode);
}
