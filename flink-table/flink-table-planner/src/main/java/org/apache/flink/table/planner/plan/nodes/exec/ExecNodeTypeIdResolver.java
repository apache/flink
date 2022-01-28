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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DatabindContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase;

/**
 * Helper class to implement the Jackson subtype serialization/deserialization. Instead of using the
 * class name use the {@link ExecNodeMetadata#name()} and {@link ExecNodeMetadata#version()} to
 * perform a lookup in a static map residing in {@link ExecNodeMetadataUtil}.
 */
@Internal
class ExecNodeTypeIdResolver extends TypeIdResolverBase {

    private JavaType superType;

    @Override
    public void init(JavaType baseType) {
        superType = baseType;
    }

    @Override
    public Id getMechanism() {
        return Id.NAME;
    }

    @Override
    public String idFromValue(Object obj) {
        return idFromValueAndType(obj, obj.getClass());
    }

    @Override
    public String idFromValueAndType(Object obj, Class<?> subType) {
        return ((ExecNodeBase<?>) obj).getContextFromAnnotation().toString();
    }

    @Override
    public JavaType typeFromId(DatabindContext context, String id) {
        ExecNodeContext execNodeContext = new ExecNodeContext(id);
        return context.constructSpecializedType(
                superType,
                ExecNodeMetadataUtil.retrieveExecNode(
                        execNodeContext.getName(), execNodeContext.getVersion()));
    }
}
