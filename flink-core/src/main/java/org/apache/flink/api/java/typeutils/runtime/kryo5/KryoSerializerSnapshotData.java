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
import org.apache.flink.api.common.ExecutionConfig.SerializableKryo5Serializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.Kryo5Registration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.LinkedOptionalMap;
import org.apache.flink.util.function.BiFunctionWithException;

import com.esotericsoftware.kryo.kryo5.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InvalidClassException;
import java.util.LinkedHashMap;
import java.util.function.Function;

import static org.apache.flink.util.LinkedOptionalMap.optionalMapOf;
import static org.apache.flink.util.LinkedOptionalMapSerializer.readOptionalMap;
import static org.apache.flink.util.LinkedOptionalMapSerializer.writeOptionalMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
final class KryoSerializerSnapshotData<T> {

    private static final Logger LOG = LoggerFactory.getLogger(KryoSerializerSnapshotData.class);

    // --------------------------------------------------------------------------------------------
    // Factories
    // --------------------------------------------------------------------------------------------

    static <T> KryoSerializerSnapshotData<T> createFrom(
            Class<T> typeClass,
            LinkedHashMap<Class<?>, SerializableKryo5Serializer<?>> defaultKryoSerializers,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses,
            LinkedHashMap<String, Kryo5Registration> kryoRegistrations) {

        return new KryoSerializerSnapshotData<>(
                typeClass,
                optionalMapOf(defaultKryoSerializers, Class::getName),
                optionalMapOf(defaultKryoSerializerClasses, Class::getName),
                optionalMapOf(kryoRegistrations, Function.identity()));
    }

    static <T> KryoSerializerSnapshotData<T> createFrom(DataInputView in, ClassLoader cl)
            throws IOException {
        Class<T> typeClass = readTypeClass(in, cl);
        LinkedOptionalMap<String, Kryo5Registration> kryoRegistrations =
                readKryoRegistrations(in, cl);
        LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>> defaultSerializer =
                readDefaultKryoSerializers(in, cl);
        LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses =
                readDefaultKryoSerializerClasses(in, cl);

        return new KryoSerializerSnapshotData<>(
                typeClass, defaultSerializer, defaultSerializerClasses, kryoRegistrations);
    }

    // --------------------------------------------------------------------------------------------
    // Fields
    // --------------------------------------------------------------------------------------------

    private final Class<T> typeClass;
    private final LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>>
            defaultKryoSerializers;
    private final LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>>
            defaultKryoSerializerClasses;
    private final LinkedOptionalMap<String, Kryo5Registration> kryoRegistrations;

    private KryoSerializerSnapshotData(
            Class<T> typeClass,
            LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>> defaultKryoSerializers,
            LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>>
                    defaultKryoSerializerClasses,
            LinkedOptionalMap<String, Kryo5Registration> kryoRegistrations) {

        this.typeClass = typeClass;
        this.defaultKryoSerializers = defaultKryoSerializers;
        this.defaultKryoSerializerClasses = defaultKryoSerializerClasses;
        this.kryoRegistrations = kryoRegistrations;
    }

    // --------------------------------------------------------------------------------------------
    // Getters
    // --------------------------------------------------------------------------------------------

    Class<T> getTypeClass() {
        return typeClass;
    }

    LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>> getDefaultKryoSerializers() {
        return defaultKryoSerializers;
    }

    LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses() {
        return defaultKryoSerializerClasses;
    }

    LinkedOptionalMap<String, Kryo5Registration> getKryoRegistrations() {
        return kryoRegistrations;
    }

    // --------------------------------------------------------------------------------------------
    // Write
    // --------------------------------------------------------------------------------------------

    void writeSnapshotData(DataOutputView out) throws IOException {
        writeTypeClass(out);
        writeKryoRegistrations(out, kryoRegistrations);
        writeDefaultKryoSerializers(out, defaultKryoSerializers);
        writeDefaultKryoSerializerClasses(out, defaultKryoSerializerClasses);
    }

    private void writeTypeClass(DataOutputView out) throws IOException {
        out.writeUTF(typeClass.getName());
    }

    private static void writeKryoRegistrations(
            DataOutputView out, LinkedOptionalMap<String, Kryo5Registration> kryoRegistrations)
            throws IOException {

        writeOptionalMap(
                out,
                kryoRegistrations,
                DataOutput::writeUTF,
                KryoRegistrationUtil::writeKryoRegistration);
    }

    private void writeDefaultKryoSerializers(
            DataOutputView out,
            LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>> defaultKryoSerializers)
            throws IOException {

        writeOptionalMap(
                out,
                defaultKryoSerializers,
                (stream, klass) -> stream.writeUTF(klass.getName()),
                (stream, instance) -> {
                    try (final DataOutputViewStream outViewWrapper =
                            new DataOutputViewStream(stream)) {
                        InstantiationUtil.serializeObject(outViewWrapper, instance);
                    }
                });
    }

    private static void writeDefaultKryoSerializerClasses(
            DataOutputView out,
            LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>>
                    defaultKryoSerializerClasses)
            throws IOException {

        writeOptionalMap(
                out,
                defaultKryoSerializerClasses,
                (stream, klass) -> stream.writeUTF(klass.getName()),
                (stream, klass) -> stream.writeUTF(klass.getName()));
    }

    // --------------------------------------------------------------------------------------------
    // Read
    // --------------------------------------------------------------------------------------------

    private static <T> Class<T> readTypeClass(DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        return InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
    }

    private static LinkedOptionalMap<String, Kryo5Registration> readKryoRegistrations(
            DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

        return readOptionalMap(
                in,
                (stream, unused) -> stream.readUTF(),
                (stream, unused) ->
                        KryoRegistrationUtil.tryReadKryoRegistration(stream, userCodeClassLoader));
    }

    @SuppressWarnings("unchecked")
    private static LinkedOptionalMap<Class<?>, SerializableKryo5Serializer<?>>
            readDefaultKryoSerializers(DataInputView in, ClassLoader cl) throws IOException {
        return readOptionalMap(
                in, new ClassResolverByName(cl), new SerializeableSerializerResolver(cl));
    }

    @SuppressWarnings("unchecked")
    private static LinkedOptionalMap<Class<?>, Class<? extends Serializer<?>>>
            readDefaultKryoSerializerClasses(DataInputView in, ClassLoader cl) throws IOException {

        return readOptionalMap(
                in, new ClassResolverByName(cl), new ClassResolverByName<Serializer<?>>(cl));
    }

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static final class KryoRegistrationUtil {

        static void writeKryoRegistration(DataOutputView out, Kryo5Registration kryoRegistration)
                throws IOException {

            checkNotNull(kryoRegistration);
            out.writeUTF(kryoRegistration.getRegisteredClass().getName());

            final Kryo5Registration.SerializerDefinitionType serializerDefinitionType =
                    kryoRegistration.getSerializerDefinitionType();

            out.writeInt(serializerDefinitionType.ordinal());
            switch (serializerDefinitionType) {
                case UNSPECIFIED:
                    {
                        // nothing else to write
                        break;
                    }
                case CLASS:
                    {
                        Class<? extends Serializer<?>> serializerClass =
                                kryoRegistration.getSerializerClass();
                        assert serializerClass != null;
                        out.writeUTF(serializerClass.getName());
                        break;
                    }
                case INSTANCE:
                    {
                        try (final DataOutputViewStream outViewWrapper =
                                new DataOutputViewStream(out)) {
                            InstantiationUtil.serializeObject(
                                    outViewWrapper,
                                    kryoRegistration.getSerializableSerializerInstance());
                        }
                        break;
                    }
                default:
                    {
                        throw new IllegalStateException(
                                "Unrecognized Kryo registration serializer definition type: "
                                        + serializerDefinitionType);
                    }
            }
        }

        static Kryo5Registration tryReadKryoRegistration(
                DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

            String registeredClassname = in.readUTF();
            Class<?> registeredClass;
            try {
                registeredClass = Class.forName(registeredClassname, true, userCodeClassLoader);
            } catch (ClassNotFoundException e) {
                LOG.warn(
                        "Cannot find registered class "
                                + registeredClassname
                                + " for Kryo serialization in classpath;"
                                + " using a dummy class as a placeholder.",
                        e);
                return null;
            }

            final Kryo5Registration.SerializerDefinitionType serializerDefinitionType =
                    Kryo5Registration.SerializerDefinitionType.values()[in.readInt()];

            switch (serializerDefinitionType) {
                case UNSPECIFIED:
                    {
                        return new Kryo5Registration(registeredClass);
                    }
                case CLASS:
                    {
                        return tryReadWithSerializerClass(
                                in, userCodeClassLoader, registeredClassname, registeredClass);
                    }
                case INSTANCE:
                    {
                        return tryReadWithSerializerInstance(
                                in, userCodeClassLoader, registeredClassname, registeredClass);
                    }
                default:
                    {
                        throw new IllegalStateException(
                                "Unrecognized Kryo registration serializer definition type: "
                                        + serializerDefinitionType);
                    }
            }
        }

        @SuppressWarnings("unchecked")
        private static Kryo5Registration tryReadWithSerializerClass(
                DataInputView in,
                ClassLoader userCodeClassLoader,
                String registeredClassname,
                Class<?> registeredClass)
                throws IOException {
            String serializerClassname = in.readUTF();
            Class serializerClass;
            try {
                serializerClass = Class.forName(serializerClassname, true, userCodeClassLoader);
                return new Kryo5Registration(registeredClass, serializerClass);
            } catch (ClassNotFoundException e) {
                LOG.warn(
                        "Cannot find registered Kryo serializer class for class "
                                + registeredClassname
                                + " in classpath; using a dummy Kryo serializer that should be replaced as soon as"
                                + " a new Kryo serializer for the class is present",
                        e);
            }
            return null;
        }

        private static Kryo5Registration tryReadWithSerializerInstance(
                DataInputView in,
                ClassLoader userCodeClassLoader,
                String registeredClassname,
                Class<?> registeredClass)
                throws IOException {
            SerializableKryo5Serializer<? extends Serializer<?>> serializerInstance;

            try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
                serializerInstance =
                        InstantiationUtil.deserializeObject(inViewWrapper, userCodeClassLoader);
                return new Kryo5Registration(registeredClass, serializerInstance);
            } catch (ClassNotFoundException e) {
                LOG.warn(
                        "Cannot find registered Kryo serializer class for class "
                                + registeredClassname
                                + " in classpath; using a dummy Kryo serializer that should be replaced as soon as"
                                + " a new Kryo serializer for the class is present",
                        e);
            } catch (InvalidClassException e) {
                LOG.warn(
                        "The registered Kryo serializer class for class "
                                + registeredClassname
                                + " has changed and is no longer valid; using a dummy Kryo serializer that should be replaced"
                                + " as soon as a new Kryo serializer for the class is present.",
                        e);
            }
            return null;
        }
    }

    private static class ClassResolverByName<T>
            implements BiFunctionWithException<DataInputView, String, Class<T>, IOException> {
        private final ClassLoader classLoader;

        private ClassResolverByName(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<T> apply(DataInputView stream, String unused) throws IOException {
            String className = stream.readUTF();
            try {
                return (Class<T>) Class.forName(className, false, classLoader);
            } catch (ClassNotFoundException e) {
                LOG.warn(
                        "Cannot find registered class "
                                + className
                                + " for Kryo serialization in classpath.",
                        e);
                return null;
            }
        }
    }

    private static final class SerializeableSerializerResolver
            implements BiFunctionWithException<
                    DataInputView, String, SerializableKryo5Serializer<?>, IOException> {

        private final ClassLoader classLoader;

        private SerializeableSerializerResolver(ClassLoader classLoader) {
            this.classLoader = classLoader;
        }

        @Override
        public SerializableKryo5Serializer<?> apply(DataInputView stream, String className) {
            try {
                try (final DataInputViewStream inViewWrapper = new DataInputViewStream(stream)) {
                    return InstantiationUtil.deserializeObject(inViewWrapper, classLoader);
                }
            } catch (Throwable e) {
                LOG.warn(
                        "Cannot deserialize a previously serialized kryo serializer for the type "
                                + className,
                        e);
                return null;
            }
        }
    }
}
