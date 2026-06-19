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

package org.apache.flink.datastream.impl.stream;

import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.datastream.api.stream.ProcessConfigurable;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;

/** A handle to configure process function related things. */
@SuppressWarnings("unchecked")
public class ProcessConfigureHandle<T, S extends ProcessConfigurable<S>>
        implements ProcessConfigurable<S> {
    protected final ExecutionEnvironmentImpl environment;

    protected final Transformation<T> transformation;

    public ProcessConfigureHandle(
            ExecutionEnvironmentImpl environment, Transformation<T> transformation) {
        this.environment = environment;
        this.transformation = transformation;
    }

    @Override
    public S withUid(String uid) {
        transformation.setUid(uid);
        return (S) this;
    }

    @Override
    public S withName(String name) {
        transformation.setName(name);
        return (S) this;
    }

    @Override
    public S withParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism, canBeParallel());
        transformation.setParallelism(parallelism);
        return (S) this;
    }

    @Override
    public S withMaxParallelism(int maxParallelism) {
        OperatorValidationUtils.validateMaxParallelism(maxParallelism, canBeParallel());
        transformation.setMaxParallelism(maxParallelism);
        return (S) this;
    }

    @Override
    public S withSlotSharingGroup(org.apache.flink.api.common.SlotSharingGroup ssg) {
        transformation.setSlotSharingGroup(SlotSharingGroup.from(ssg));
        return (S) this;
    }

    protected boolean canBeParallel() {
        return true;
    }
}
