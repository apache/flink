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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.twitter.chill.java.ArraysAsListSerializer;
import com.twitter.chill.java.BitSetSerializer;
import com.twitter.chill.java.InetSocketAddressSerializer;
import com.twitter.chill.java.LocaleSerializer;
import com.twitter.chill.java.RegexSerializer;
import com.twitter.chill.java.SimpleDateFormatSerializer;
import com.twitter.chill.java.SqlDateSerializer;
import com.twitter.chill.java.SqlTimeSerializer;
import com.twitter.chill.java.TimestampSerializer;
import com.twitter.chill.java.URISerializer;
import com.twitter.chill.java.UUIDSerializer;

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
 * Registers all chill serializers used for Java types.
 *
 * <p>All registrations use a hard-coded ID which were determined at commit
 * 18f176ce86900fd4e932c73f3d138912355c6880.
 */
public class FlinkChillPackageRegistrar implements ChillSerializerRegistrar {

    private static final int FIRST_REGISTRATION_ID = 73;

    @Override
    public int getNextRegistrationId() {
        return 85;
    }

    @Override
    public void registerSerializers(Kryo kryo) {
        //noinspection ArraysAsListWithZeroOrOneArgument
        new RegistrationHelper(FIRST_REGISTRATION_ID, kryo)
                .register(Arrays.asList("").getClass(), new ArraysAsListSerializer())
                .register(BitSet.class, new BitSetSerializer())
                .register(PriorityQueue.class, new PriorityQueueSerializer())
                .register(Pattern.class, new RegexSerializer())
                .register(Date.class, new SqlDateSerializer())
                .register(Time.class, new SqlTimeSerializer())
                .register(Timestamp.class, new TimestampSerializer())
                .register(URI.class, new URISerializer())
                .register(InetSocketAddress.class, new InetSocketAddressSerializer())
                .register(UUID.class, new UUIDSerializer())
                .register(Locale.class, new LocaleSerializer())
                .register(SimpleDateFormat.class, new SimpleDateFormatSerializer());
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
