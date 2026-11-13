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

package org.apache.flink.api.common.errors;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Who has to act on a {@link FlinkError}.
 *
 * <p>The category belongs to the error, not to the class of the exception that carries it. For
 * example, a {@code ValidationException} can carry an internal error.
 *
 * <p>Values may be added in later versions.
 */
@PublicEvolving
public enum ErrorCategory {

    /**
     * The user has to change the statement, the query or the configuration, for example because a
     * table doesn't exist or an option has an invalid value.
     */
    USER,

    /**
     * A bug in Flink. A condition that Flink relies on isn't met, similar to an {@link
     * IllegalStateException}, for example an input that a rule of the planner can't handle.
     */
    INTERNAL,

    /**
     * A system outside Flink failed, for example the backend of a catalog that can't be reached or
     * an external system that rejects a request of a connector.
     */
    EXTERNAL
}
