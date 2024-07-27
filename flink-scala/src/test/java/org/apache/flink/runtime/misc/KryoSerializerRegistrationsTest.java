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

package org.apache.flink.runtime.misc;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests that the set of Kryo registrations is the same across compatible Flink versions.
 *
 * <p>The test needs to be in the runtime package, rather than in the core/java package where the
 * Kryo serializer itself sits, because when runtime is present in the classpath, Chill is used to
 * instantiate Kryo and adds the proper set of registrations.
 */
class KryoSerializerRegistrationsTest {

    /**
     * Tests that the registered classes in Kryo did not change.
     *
     * <p>Once we have proper serializer versioning this test will become obsolete. But currently a
     * change in the serializers can break savepoint backwards compatibility between Flink versions.
     */
    @Test
    void testDefaultKryoRegisteredClassesDidNotChange() throws Exception {
        final Kryo kryo = new KryoSerializer<>(Integer.class, new SerializerConfigImpl()).getKryo();

        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                getClass()
                                        .getClassLoader()
                                        .getResourceAsStream("flink_11-kryo_registrations")))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                final int tag = Integer.parseInt(split[0]);
                final String registeredClass = split[1];

                Registration registration = kryo.getRegistration(tag);

                if (registration == null) {
                    fail(String.format("Registration for %d = %s got lost", tag, registeredClass));
                } else if (registeredClass.equals("org.apache.avro.generic.GenericData$Array")) {
                    // starting with Flink 1.4 Avro is no longer a dependency of core. Avro is
                    // only available if flink-avro is present. There is a special version of
                    // this test in AvroKryoSerializerRegistrationsTest that verifies correct
                    // registration of Avro types if present
                    assertThat(registration.getType().getName())
                            .isEqualTo(
                                    "org.apache.flink.api.java.typeutils.runtime.kryo.Serializers$DummyAvroRegisteredClass");
                } else if (!registeredClass.equals(registration.getType().getName())) {
                    fail(
                            String.format(
                                    "Registration for %d = %s changed to %s",
                                    tag, registeredClass, registration.getType().getName()));
                }
            }
        }
    }
}
