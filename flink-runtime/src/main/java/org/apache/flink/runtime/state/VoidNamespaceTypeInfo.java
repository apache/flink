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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** {@link TypeInformation} for {@link VoidNamespace}. */
@Public
public class VoidNamespaceTypeInfo extends TypeInformation<VoidNamespace> {

    private static final long serialVersionUID = 5453679706408610586L;

    public static final VoidNamespaceTypeInfo INSTANCE = new VoidNamespaceTypeInfo();

    @Override
    @PublicEvolving
    public boolean isBasicType() {
        return false;
    }

    @Override
    @PublicEvolving
    public boolean isTupleType() {
        return false;
    }

    @Override
    @PublicEvolving
    public int getArity() {
        return 0;
    }

    @Override
    @PublicEvolving
    public int getTotalFields() {
        return 1;
    }

    @Override
    @PublicEvolving
    public Class<VoidNamespace> getTypeClass() {
        return VoidNamespace.class;
    }

    @Override
    @PublicEvolving
    public boolean isKeyType() {
        return false;
    }

    @Override
    @PublicEvolving
    public TypeSerializer<VoidNamespace> createSerializer(ExecutionConfig config) {
        return VoidNamespaceSerializer.INSTANCE;
    }

    @Override
    @PublicEvolving
    public String toString() {
        return "VoidNamespaceTypeInfo";
    }

    @Override
    @PublicEvolving
    public boolean equals(Object obj) {
        return this == obj || obj instanceof VoidNamespaceTypeInfo;
    }

    @Override
    @PublicEvolving
    public int hashCode() {
        return 0;
    }

    @Override
    @PublicEvolving
    public boolean canEqual(Object obj) {
        return obj instanceof VoidNamespaceTypeInfo;
    }
}
