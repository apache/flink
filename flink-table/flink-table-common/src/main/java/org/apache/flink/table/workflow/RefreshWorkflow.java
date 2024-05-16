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
import org.apache.flink.table.catalog.CatalogMaterializedTable;

/**
 * {@link RefreshWorkflow} is the basic interface that provide the related information to operate
 * the refresh workflow of {@link CatalogMaterializedTable}, the operation of refresh workflow
 * include create, modify, drop, etc.
 *
 * @see CreateRefreshWorkflow
 * @see ModifyRefreshWorkflow
 * @see DeleteRefreshWorkflow
 */
@PublicEvolving
public interface RefreshWorkflow {}
