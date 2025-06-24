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

package org.apache.flink.table.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;

/**
 * This interface is used to interact with specific workflow scheduler services that support
 * creating, modifying, and deleting refreshed workflow of Materialized Table.
 *
 * @param <T> The type of {@link RefreshHandler} used by specific {@link WorkflowScheduler} to
 *     locate the refresh workflow in scheduler service.
 */
@PublicEvolving
public interface WorkflowScheduler<T extends RefreshHandler> {

    /**
     * Open this workflow scheduler instance. Used for any required preparation in initialization
     * phase.
     *
     * @throws WorkflowException if initializing workflow scheduler occur exception
     */
    void open() throws WorkflowException;

    /**
     * Close this workflow scheduler when it is no longer needed and release any resource that it
     * might be holding.
     *
     * @throws WorkflowException if closing the related resources of workflow scheduler failed
     */
    void close() throws WorkflowException;

    /**
     * Return a {@link RefreshHandlerSerializer} instance to serialize and deserialize {@link
     * RefreshHandler} created by specific workflow scheduler service.
     */
    RefreshHandlerSerializer<T> getRefreshHandlerSerializer();

    /**
     * Create a refresh workflow in specific scheduler service for the materialized table, return a
     * {@link RefreshHandler} instance which can locate the refresh workflow detail information.
     *
     * <p>This method supports creating workflow for periodic refresh, as well as workflow for a
     * one-time refresh only.
     *
     * @param createRefreshWorkflow The detail info for create refresh workflow of materialized
     *     table.
     * @return The meta info which points to the refresh workflow in scheduler service.
     * @throws WorkflowException if creating refresh workflow failed
     */
    T createRefreshWorkflow(CreateRefreshWorkflow createRefreshWorkflow) throws WorkflowException;

    /**
     * Modify the refresh workflow status in scheduler service. This includes suspend, resume,
     * modify schedule cron operation, and so on.
     *
     * @param modifyRefreshWorkflow The detail info for modify refresh workflow of materialized
     *     table.
     * @throws WorkflowException if modify refresh workflow failed
     */
    void modifyRefreshWorkflow(ModifyRefreshWorkflow<T> modifyRefreshWorkflow)
            throws WorkflowException;

    /**
     * Delete the refresh workflow in scheduler service.
     *
     * @param deleteRefreshWorkflow The detail info for delete refresh workflow of materialized
     *     table.
     * @throws WorkflowException if delete refresh workflow failed
     */
    void deleteRefreshWorkflow(DeleteRefreshWorkflow<T> deleteRefreshWorkflow)
            throws WorkflowException;
}
