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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VariantSerializer;
import org.apache.flink.types.variant.Variant;

/** Type information for Variant. */
@PublicEvolving
public class VariantTypeInfo extends TypeInformation<Variant> {

    public static final VariantTypeInfo INSTANCE = new VariantTypeInfo();

    private VariantTypeInfo() {}

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<Variant> getTypeClass() {
        return Variant.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public TypeSerializer<Variant> createSerializer(SerializerConfig config) {
        return VariantSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return Variant.class.getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VariantTypeInfo) {
            VariantTypeInfo other = (VariantTypeInfo) obj;
            return other.canEqual(this);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return VariantTypeInfo.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof VariantTypeInfo;
    }
}
