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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.util.CollectionUtil;

import java.util.Map;

/**
 * Helper to access {@link MetadataSerializer}s for specific format versions.
 *
 * <p>The serializer for a specific version can be obtained via {@link #getSerializer(int)}.
 */
public class MetadataSerializers {

    private static final Map<Integer, MetadataSerializer> SERIALIZERS =
            CollectionUtil.newHashMapWithExpectedSize(4);

    static {
        registerSerializer(MetadataV1Serializer.INSTANCE);
        registerSerializer(MetadataV2Serializer.INSTANCE);
        registerSerializer(MetadataV3Serializer.INSTANCE);
        registerSerializer(MetadataV4Serializer.INSTANCE);
    }

    private static void registerSerializer(MetadataSerializer serializer) {
        SERIALIZERS.put(serializer.getVersion(), serializer);
    }

    /**
     * Returns the {@link MetadataSerializer} for the given savepoint version.
     *
     * @param version Savepoint version to get serializer for
     * @return Savepoint for the given version
     * @throws IllegalArgumentException If unknown savepoint version
     */
    public static MetadataSerializer getSerializer(int version) {
        MetadataSerializer serializer = SERIALIZERS.get(version);
        if (serializer != null) {
            return serializer;
        } else {
            throw new IllegalArgumentException(
                    "Unrecognized checkpoint version number: " + version);
        }
    }

    // ------------------------------------------------------------------------

    /** Utility method class, not meant to be instantiated. */
    private MetadataSerializers() {}
}
