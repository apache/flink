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

package org.apache.flink.types.variant;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.flink.types.variant.BinaryVariantUtil.encodeKey;

/**
 * Reads one field of object variants by a key that is known up front, such as {@code 'user'} in
 * {@code v['user']}.
 *
 * <p>The getter remembers the index at which it last found the key and checks it first. Objects of
 * one stream mostly share their keys, so the key is usually at the same index. A wrong guess costs
 * one key comparison before the regular search.
 *
 * <p>The remembered index is only a hint that is always verified, so a getter is safe to share
 * between threads.
 */
@Internal
public final class VariantFieldGetter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] key;
    // Read and written by BinaryVariant#getFieldByKey. -1 until the getter has found its key.
    int lastIndex = -1;

    public VariantFieldGetter(final String fieldName) {
        this.key = encodeKey(fieldName);
    }

    /**
     * Returns the field of an object variant like {@link Variant#getField}, or null if the object
     * has no such field. For an object that holds the key twice, the field it returns may differ
     * between calls.
     *
     * @throws VariantTypeException if the variant is not an object
     */
    public @Nullable Variant getField(final Variant variant) {
        return ((BinaryVariant) variant).getFieldByKey(key, this);
    }
}
