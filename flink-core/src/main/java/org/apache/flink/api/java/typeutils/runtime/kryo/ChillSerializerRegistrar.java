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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;

/** Interface for flink-core to interact with the FlinkChillPackageRegistrar in flink-java. */
public interface ChillSerializerRegistrar {
    /**
     * Registers all serializers with the given {@link Kryo}. All serializers are registered with
     * specific IDs as a continuous block.
     *
     * @param kryo Kryo to register serializers with
     */
    void registerSerializers(Kryo kryo);

    /**
     * Returns the registration ID that immediately follows the last registered serializer.
     *
     * @return registration ID that should be used for the next serializer registration
     */
    int getNextRegistrationId();
}
