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

package org.apache.flink.api.common.typeinfo.descriptor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Implementation of {@link TypeDescriptor} to create {@link BasicTypeInfo}. Note that this class is
 * initiated via reflection. So, changing its path or constructor will brake tests.
 *
 * @param <T> type for which {@link TypeInformation} is created.
 */
@Internal
public class BasicTypeDescriptorImpl<T> implements TypeDescriptor<T> {

    private final BasicTypeInfo<T> basicTypeInfo;

    public BasicTypeDescriptorImpl(TypeDescriptor<T> typeDescriptor) {
        basicTypeInfo = BasicTypeInfo.getInfoFor(typeDescriptor.getTypeClass());
    }

    public BasicTypeInfo<T> getBasicTypeInfo() {
        return basicTypeInfo;
    }

    @Override
    public Class<T> getTypeClass() {
        return basicTypeInfo.getTypeClass();
    }

    @Override
    public String toString() {
        return "BasicTypeDescriptorImpl [basicTypeInfo=" + basicTypeInfo + "]";
    }
}
