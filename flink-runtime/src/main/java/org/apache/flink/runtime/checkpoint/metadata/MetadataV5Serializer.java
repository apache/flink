/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;

/**
 * V5 serializer that adds {@link org.apache.flink.runtime.checkpoint.CheckpointProperties}
 * serialization.
 */
@Internal
public class MetadataV5Serializer extends MetadataWithPropertiesSerializer {

    public static final MetadataSerializer INSTANCE = new MetadataV5Serializer();
    public static final int VERSION = 5;

    private static final MetadataV3Serializer baseSerializer =
            new MetadataV3Serializer(new ChannelStateHandleSerializerV2());

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    protected MetadataSerializer basedSerializer() {
        return baseSerializer;
    }
}
