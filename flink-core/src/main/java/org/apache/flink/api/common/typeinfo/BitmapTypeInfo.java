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
import org.apache.flink.api.common.typeutils.base.BitmapSerializer;
import org.apache.flink.types.bitmap.Bitmap;

/** Type information for {@link Bitmap}. */
@PublicEvolving
public class BitmapTypeInfo extends TypeInformation<Bitmap> {

    private static final long serialVersionUID = 1L;

    public static final BitmapTypeInfo INSTANCE = new BitmapTypeInfo();

    private BitmapTypeInfo() {}

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
    public Class<Bitmap> getTypeClass() {
        return Bitmap.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public boolean isSortKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<Bitmap> createSerializer(SerializerConfig config) {
        return BitmapSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return Bitmap.class.getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BitmapTypeInfo) {
            BitmapTypeInfo other = (BitmapTypeInfo) obj;
            return other.canEqual(this);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return BitmapTypeInfo.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof BitmapTypeInfo;
    }
}
