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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SerializedCompositeKeyBuilder;

import java.io.IOException;

/** A utility of serialization keys in ForSt. */
public class ForStSerializerUtils {

    /**
     * Serialize a key and namespace. No user key.
     *
     * @param contextKey the context key of current request
     * @param builder key builder
     * @param defaultNamespace default namespace of the state
     * @param namespaceSerializer the namespace serializer
     * @param enableKeyReuse whether to enable key reuse
     */
    public static <K, N> byte[] serializeKeyAndNamespace(
            ContextKey<K, N> contextKey,
            SerializedCompositeKeyBuilder<K> builder,
            N defaultNamespace,
            TypeSerializer<N> namespaceSerializer,
            boolean enableKeyReuse)
            throws IOException {
        N namespace = contextKey.getNamespace();
        namespace = (namespace == null ? defaultNamespace : namespace);
        if (enableKeyReuse && namespace == defaultNamespace) {
            // key reuse.
            return contextKey.getOrCreateSerializedKey(
                    ctxKey -> {
                        builder.setKeyAndKeyGroup(ctxKey.getRawKey(), ctxKey.getKeyGroup());
                        return builder.buildCompositeKeyNamespace(
                                defaultNamespace, namespaceSerializer);
                    });
        } else {
            // no key reuse, serialize again.
            builder.setKeyAndKeyGroup(contextKey.getRawKey(), contextKey.getKeyGroup());
            return builder.buildCompositeKeyNamespace(namespace, namespaceSerializer);
        }
    }

    private ForStSerializerUtils() {}
}
