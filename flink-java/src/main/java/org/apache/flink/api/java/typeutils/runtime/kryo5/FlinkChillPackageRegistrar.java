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

package org.apache.flink.api.java.typeutils.runtime.kryo5;

import org.apache.flink.annotation.PublicEvolving;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.Serializer;
import com.esotericsoftware.kryo.kryo5.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.kryo5.serializers.ImmutableCollectionsSerializers;
import com.esotericsoftware.kryo.kryo5.serializers.JavaSerializer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * This is a Kryo 5 port of a Kryo 2 utility that registered Kryo 2 serializers from the Chill
 * library. This version doesn't use Chill but registers Kryo 5 serializers for the same data types
 * with the same ids.
 *
 * <p>All registrations use a hard-coded ID which were determined at commit
 * 18f176ce86900fd4e932c73f3d138912355c6880.
 */
@PublicEvolving
public class FlinkChillPackageRegistrar implements ChillSerializerRegistrar {

    private static final int FIRST_REGISTRATION_ID = 73;

    @Override
    public int getNextRegistrationId() {
        return 85;
    }

    @Override
    public void registerSerializers(Kryo kryo) {
        //noinspection ArraysAsListWithZeroOrOneArgument
        ImmutableCollectionsSerializers.addDefaultSerializers(kryo);

        new RegistrationHelper(FIRST_REGISTRATION_ID, kryo)
                .register(
                        Arrays.asList("").getClass(),
                        new DefaultSerializers.ArraysAsListSerializer())
                .register(BitSet.class, new DefaultSerializers.BitSetSerializer())
                .register(PriorityQueue.class, new DefaultSerializers.PriorityQueueSerializer())
                .register(Pattern.class, new DefaultSerializers.PatternSerializer())
                .register(Date.class, new DefaultSerializers.DateSerializer())
                .register(Time.class, new DefaultSerializers.DateSerializer())
                .register(Timestamp.class, new DefaultSerializers.TimestampSerializer())
                .register(URI.class, new DefaultSerializers.URISerializer())
                .register(InetSocketAddress.class, new InetSocketAddressSerializer())
                .register(UUID.class, new DefaultSerializers.UUIDSerializer())
                .register(Locale.class, new DefaultSerializers.LocaleSerializer())
                .register(SimpleDateFormat.class, new JavaSerializer());
    }

    private static final class RegistrationHelper {
        private int nextRegistrationId;
        private final Kryo kryo;

        public RegistrationHelper(int firstRegistrationId, Kryo kryo) {
            this.nextRegistrationId = firstRegistrationId;
            this.kryo = kryo;
        }

        public RegistrationHelper register(Class<?> type, Serializer<?> serializer) {
            kryo.register(type, serializer, nextRegistrationId++);
            return this;
        }
    }
}
