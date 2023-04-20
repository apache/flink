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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Constraint.ConstraintType;
import org.apache.flink.table.catalog.UniqueConstraint;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** Mixin for {@link UniqueConstraint}. */
@Internal
abstract class UniqueConstraintMixin {

    static final String NAME = "name";
    static final String ENFORCED = "enforced";
    static final String TYPE = "type";
    static final String COLUMNS = "columns";

    @JsonCreator
    private UniqueConstraintMixin(
            @JsonProperty(NAME) String name,
            @JsonProperty(ENFORCED) boolean enforced,
            @JsonProperty(TYPE) ConstraintType type,
            @JsonProperty(COLUMNS) List<String> columns) {}

    @JsonProperty(NAME)
    public abstract String getName();

    @JsonProperty(TYPE)
    public abstract ConstraintType getType();

    @JsonProperty(ENFORCED)
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public abstract boolean isEnforced();

    @JsonProperty(COLUMNS)
    public abstract List<String> getColumns();
}
