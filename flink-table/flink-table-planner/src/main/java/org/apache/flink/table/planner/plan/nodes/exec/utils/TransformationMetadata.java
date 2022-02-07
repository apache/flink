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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;

import javax.annotation.Nullable;

/**
 * This POJO is meant to hold some metadata information about operators, which usually needs to be
 * passed to "factory" methods for {@link Transformation}.
 */
public class TransformationMetadata {
    private final @Nullable String uid;
    private final String name;
    private final String desc;

    /** Used by {@link BatchExecNode}, as they don't require the uid. */
    public TransformationMetadata(String name, String desc) {
        this(null, name, desc);
    }

    /** Used by {@link StreamExecNode}, as they require the uid. */
    public TransformationMetadata(String uid, String name, String desc) {
        this.uid = uid;
        this.name = name;
        this.desc = desc;
    }

    public @Nullable String getUid() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return desc;
    }

    /** Fill a transformation with this meta. */
    public <T extends Transformation<?>> T fill(T transformation) {
        transformation.setName(getName());
        transformation.setDescription(getDescription());
        if (getUid() != null) {
            transformation.setUid(getUid());
        }
        return transformation;
    }
}
