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
import org.apache.flink.table.delegation.Planner;

/**
 * Determine the type of the {@link Planner}. Except for the optimization, the different planner
 * also differs in the time semantic and so on.
 *
 * @deprecated The old planner has been removed in Flink 1.14. Since there is only one planner left
 *     (previously called the 'blink' planner), this class is obsolete and will be removed in future
 *     versions.
 */
@PublicEvolving
@Deprecated
public enum PlannerType {
    /** Blink planner is the up-to-date planner in Flink. */
    BLINK,

    /** Old planner is used before. It will not be maintained in the future. */
    OLD
}
