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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;

import java.util.LinkedHashMap;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.hasSuperclass;

/**
 * Utility methods for dealing with Avro types. This has a default implementation for the case that
 * Avro is not present on the classpath and an actual implementation in flink-avro that is
 * dynamically loaded when present.
 */
public abstract class AvroUtils {

    private static final String AVRO_KRYO_UTILS =
            "org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils";

    /**
     * Returns either the default {@link AvroUtils} which throw an exception in cases where Avro
     * would be needed or loads the specific utils for Avro from flink-avro.
     */
    public static AvroUtils getAvroUtils() {
        // try and load the special AvroUtils from the flink-avro package
        try {
            Class<?> clazz =
                    Class.forName(
                            AVRO_KRYO_UTILS, false, Thread.currentThread().getContextClassLoader());
            return clazz.asSubclass(AvroUtils.class).getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            // cannot find the utils, return the default implementation
            return new DefaultAvroUtils();
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate " + AVRO_KRYO_UTILS + ".", e);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Loads the utility class from <code>flink-avro</code> and adds Avro-specific serializers. This
     * method will throw an exception if we see an Avro type but flink-avro is not in the classpath.
     */
    public abstract void addAvroSerializersIfRequired(ExecutionConfig reg, Class<?> type);

    /** Registers a special Serializer for GenericData.Array. */
    public abstract void addAvroGenericDataArrayRegistration(
            LinkedHashMap<String, KryoRegistration> kryoRegistrations);

    /**
     * Creates an {@code AvroSerializer} if flink-avro is present, otherwise throws an exception.
     */
    public abstract <T> TypeSerializer<T> createAvroSerializer(Class<T> type);

    /** Creates an {@code AvroTypeInfo} if flink-avro is present, otherwise throws an exception. */
    public abstract <T> TypeInformation<T> createAvroTypeInfo(Class<T> type);

    // ------------------------------------------------------------------------

    /** A default implementation of the AvroUtils used in the absence of Avro. */
    private static class DefaultAvroUtils extends AvroUtils {

        private static final String AVRO_SPECIFIC_RECORD_BASE =
                "org.apache.avro.specific.SpecificRecordBase";

        private static final String AVRO_GENERIC_RECORD =
                "org.apache.avro.generic.GenericData$Record";

        private static final String AVRO_GENERIC_DATA_ARRAY =
                "org.apache.avro.generic.GenericData$Array";

        @Override
        public void addAvroSerializersIfRequired(ExecutionConfig reg, Class<?> type) {
            if (hasSuperclass(type, AVRO_SPECIFIC_RECORD_BASE)
                    || hasSuperclass(type, AVRO_GENERIC_RECORD)) {

                throw new RuntimeException(
                        "Could not load class '"
                                + AVRO_KRYO_UTILS
                                + "'. "
                                + "You may be missing the 'flink-avro' dependency.");
            }
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        @Override
        public void addAvroGenericDataArrayRegistration(
                LinkedHashMap<String, KryoRegistration> kryoRegistrations) {
            kryoRegistrations.put(
                    AVRO_GENERIC_DATA_ARRAY,
                    new KryoRegistration(
                            Serializers.DummyAvroRegisteredClass.class,
                            (Class) Serializers.DummyAvroKryoSerializerClass.class));
        }

        @Override
        public <T> TypeSerializer<T> createAvroSerializer(Class<T> type) {
            throw new RuntimeException(
                    "Could not load the AvroSerializer class. "
                            + "You may be missing the 'flink-avro' dependency.");
        }

        @Override
        public <T> TypeInformation<T> createAvroTypeInfo(Class<T> type) {
            throw new RuntimeException(
                    "Could not load the AvroTypeInfo class. "
                            + "You may be missing the 'flink-avro' dependency.");
        }
    }
}
