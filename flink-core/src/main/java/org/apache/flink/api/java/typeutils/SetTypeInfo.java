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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SetSerializer;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for the set types of the Java API.
 *
 * @param <T> The type of the elements in the set.
 */
@PublicEvolving
public class SetTypeInfo<T> extends TypeInformation<Set<T>> {

    private static final long serialVersionUID = 1L;

    private final TypeInformation<T> elementTypeInfo;

    public SetTypeInfo(Class<T> elementTypeClass) {
        this.elementTypeInfo =
                of(checkNotNull(elementTypeClass, "The element type class cannot be null."));
    }

    public SetTypeInfo(TypeInformation<T> elementTypeInfo) {
        this.elementTypeInfo =
                checkNotNull(elementTypeInfo, "The element type information cannot be null.");
    }

    // ------------------------------------------------------------------------
    //  SetTypeInfo specific properties
    // ------------------------------------------------------------------------

    /** Gets the type information for the elements contained in the set. */
    public TypeInformation<T> getElementTypeInfo() {
        return elementTypeInfo;
    }

    // ------------------------------------------------------------------------
    //  TypeInformation implementation
    // ------------------------------------------------------------------------

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
        return 0;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<Set<T>> getTypeClass() {
        return (Class<Set<T>>) (Class<?>) Set.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Set<T>> createSerializer(SerializerConfig config) {
        TypeSerializer<T> elementTypeSerializer = elementTypeInfo.createSerializer(config);
        return new SetSerializer<>(elementTypeSerializer);
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "Set<" + elementTypeInfo + '>';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof SetTypeInfo) {
            final SetTypeInfo<?> other = (SetTypeInfo<?>) obj;
            return other.canEqual(this) && elementTypeInfo.equals(other.elementTypeInfo);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * elementTypeInfo.hashCode() + 1;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj != null && obj.getClass() == getClass();
    }
}
