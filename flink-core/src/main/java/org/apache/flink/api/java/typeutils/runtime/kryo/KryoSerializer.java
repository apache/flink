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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig.SerializableSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.AvroUtils;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.api.java.typeutils.runtime.KryoRegistration;
import org.apache.flink.api.java.typeutils.runtime.KryoUtils;
import org.apache.flink.api.java.typeutils.runtime.NoFetchingInput;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.exception.CloneFailedException;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A type serializer that serializes its type using the Kryo serialization framework
 * (https://github.com/EsotericSoftware/kryo).
 *
 * <p>This serializer is intended as a fallback serializer for the cases that are not covered by the
 * basic types, tuples, and POJOs.
 *
 * <p>The set of serializers registered with Kryo via {@link Kryo#register}, with their respective
 * IDs, depends on whether flink-java or flink-scala are on the classpath. This is for
 * backwards-compatibility reasons.
 *
 * <p>If neither are available (which should only apply to tests in flink-core), then:
 *
 * <ul>
 *   <li>0-9 are used for Java primitives
 *   <li>10+ are used for user-defined registration
 * </ul>
 *
 * <p>If flink-scala is available, then:
 *
 * <ul>
 *   <li>0-9 are used for Java primitives
 *   <li>10-72 are used for Scala classes
 *   <li>73-84 are used for Java classes
 *   <li>85+ are used for user-defined registration
 * </ul>
 *
 * <p>If *only* flink-java is available, then:
 *
 * <ul>
 *   <li>0-9 are used for Java primitives
 *   <li>10-72 are unused (to maintain compatibility)
 *   <li>73-84 are used for Java classes
 *   <li>85+ are used for user-defined registration
 * </ul>
 *
 * @param <T> The type to be serialized.
 */
public class KryoSerializer<T> extends TypeSerializer<T> {

    private static final long serialVersionUID = 3L;

    private static final Logger LOG = LoggerFactory.getLogger(KryoSerializer.class);

    /**
     * Flag whether to check for concurrent thread access. Because this flag is static final, a
     * value of 'false' allows the JIT compiler to eliminate the guarded code sections.
     */
    private static final boolean CONCURRENT_ACCESS_CHECK =
            LOG.isDebugEnabled() || KryoSerializerDebugInitHelper.setToDebug;

    static {
        configureKryoLogging();
    }

    @Nullable
    private static final ChillSerializerRegistrar flinkChillPackageRegistrar =
            loadFlinkChillPackageRegistrar();

    @Nullable
    private static ChillSerializerRegistrar loadFlinkChillPackageRegistrar() {
        try {
            return (ChillSerializerRegistrar)
                    Class.forName(
                                    "org.apache.flink.api.java.typeutils.runtime.kryo.FlinkChillPackageRegistrar")
                            .getDeclaredConstructor()
                            .newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    // ------------------------------------------------------------------------

    private final LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            defaultSerializers;
    private final LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses;

    /**
     * Map of class tag (using classname as tag) to their Kryo registration.
     *
     * <p>This map serves as a preview of the final registration result of the Kryo instance, taking
     * into account registration overwrites.
     */
    private LinkedHashMap<String, KryoRegistration> kryoRegistrations;

    private final Class<T> type;

    // ------------------------------------------------------------------------
    // The fields below are lazily initialized after duplication or deserialization.

    private transient Kryo kryo;
    private transient T copyInstance;

    private transient DataOutputView previousOut;
    private transient DataInputView previousIn;

    private transient Input input;
    private transient Output output;

    // ------------------------------------------------------------------------
    // legacy fields; these fields cannot yet be removed to retain backwards compatibility

    private LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
            registeredTypesWithSerializers;
    private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
            registeredTypesWithSerializerClasses;
    private LinkedHashSet<Class<?>> registeredTypes;

    // for debugging purposes
    private transient volatile Thread currentThread;

    // ------------------------------------------------------------------------

    public KryoSerializer(Class<T> type, ExecutionConfig executionConfig) {
        this.type = checkNotNull(type);

        this.defaultSerializers = executionConfig.getDefaultKryoSerializers();
        this.defaultSerializerClasses = executionConfig.getDefaultKryoSerializerClasses();

        this.kryoRegistrations =
                buildKryoRegistrations(
                        this.type,
                        executionConfig.getRegisteredKryoTypes(),
                        executionConfig.getRegisteredTypesWithKryoSerializerClasses(),
                        executionConfig.getRegisteredTypesWithKryoSerializers());
    }

    /**
     * Copy-constructor that does not copy transient fields. They will be initialized once required.
     */
    protected KryoSerializer(KryoSerializer<T> toCopy) {

        this.type = checkNotNull(toCopy.type, "Type class cannot be null.");
        this.defaultSerializerClasses = toCopy.defaultSerializerClasses;
        this.defaultSerializers =
                CollectionUtil.newLinkedHashMapWithExpectedSize(toCopy.defaultSerializers.size());
        this.kryoRegistrations =
                CollectionUtil.newLinkedHashMapWithExpectedSize(toCopy.kryoRegistrations.size());

        // deep copy the serializer instances in defaultSerializers
        for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> entry :
                toCopy.defaultSerializers.entrySet()) {

            this.defaultSerializers.put(entry.getKey(), deepCopySerializer(entry.getValue()));
        }

        // deep copy the serializer instances in kryoRegistrations
        for (Map.Entry<String, KryoRegistration> entry : toCopy.kryoRegistrations.entrySet()) {

            KryoRegistration kryoRegistration = entry.getValue();

            if (kryoRegistration.getSerializerDefinitionType()
                    == KryoRegistration.SerializerDefinitionType.INSTANCE) {

                ExecutionConfig.SerializableSerializer<? extends Serializer<?>> serializerInstance =
                        kryoRegistration.getSerializableSerializerInstance();

                if (serializerInstance != null) {
                    kryoRegistration =
                            new KryoRegistration(
                                    kryoRegistration.getRegisteredClass(),
                                    deepCopySerializer(serializerInstance));
                }
            }

            this.kryoRegistrations.put(entry.getKey(), kryoRegistration);
        }
    }

    // for KryoSerializerSnapshot
    // ------------------------------------------------------------------------

    KryoSerializer(
            Class<T> type,
            LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultSerializers,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultSerializerClasses,
            LinkedHashMap<String, KryoRegistration> kryoRegistrations) {

        this.type = checkNotNull(type, "Type class cannot be null.");
        this.defaultSerializerClasses =
                checkNotNull(
                        defaultSerializerClasses, "Default serializer classes cannot be null.");
        this.defaultSerializers =
                checkNotNull(defaultSerializers, "Default serializers cannot be null.");
        this.kryoRegistrations =
                checkNotNull(kryoRegistrations, "Kryo registrations cannot be null.");
    }

    Class<T> getType() {
        return type;
    }

    LinkedHashMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers() {
        return defaultSerializers;
    }

    LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses() {
        return defaultSerializerClasses;
    }

    LinkedHashMap<String, KryoRegistration> getKryoRegistrations() {
        return kryoRegistrations;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public KryoSerializer<T> duplicate() {
        return new KryoSerializer<>(this);
    }

    @Override
    public T createInstance() {
        if (Modifier.isAbstract(type.getModifiers()) || Modifier.isInterface(type.getModifiers())) {
            return null;
        } else {
            checkKryoInitialized();
            try {
                return kryo.newInstance(type);
            } catch (Throwable e) {
                return null;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T copy(T from) {
        if (from == null) {
            return null;
        }

        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();
            try {
                return kryo.copy(from);
            } catch (KryoException ke) {
                // kryo was unable to copy it, so we do it through serialization:
                ByteArrayOutputStream baout = new ByteArrayOutputStream();
                Output output = new Output(baout);

                kryo.writeObject(output, from);

                output.close();

                ByteArrayInputStream bain = new ByteArrayInputStream(baout.toByteArray());
                Input input = new Input(bain);

                return (T) kryo.readObject(input, from.getClass());
            }
        } finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();

            if (target != previousOut) {
                DataOutputViewStream outputStream = new DataOutputViewStream(target);
                output = new Output(outputStream);
                previousOut = target;
            }

            // Sanity check: Make sure that the output is cleared/has been flushed by the last call
            // otherwise data might be written multiple times in case of a previous EOFException
            if (output.position() != 0) {
                throw new IllegalStateException(
                        "The Kryo Output still contains data from a previous "
                                + "serialize call. It has to be flushed or cleared at the end of the serialize call.");
            }

            try {
                kryo.writeClassAndObject(output, record);
                output.flush();
            } catch (KryoException ke) {
                // make sure that the Kryo output buffer is cleared in case that we can recover from
                // the exception (e.g. EOFException which denotes buffer full)
                output.clear();

                Throwable cause = ke.getCause();
                if (cause instanceof EOFException) {
                    throw (EOFException) cause;
                } else {
                    throw ke;
                }
            }
        } finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(DataInputView source) throws IOException {
        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();

            if (source != previousIn) {
                DataInputViewStream inputStream = new DataInputViewStream(source);
                input = new NoFetchingInput(inputStream);
                previousIn = source;
            }

            try {
                return (T) kryo.readClassAndObject(input);
            } catch (KryoException ke) {
                Throwable cause = ke.getCause();

                if (cause instanceof EOFException) {
                    throw (EOFException) cause;
                } else {
                    throw ke;
                }
            }
        } finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        if (CONCURRENT_ACCESS_CHECK) {
            enterExclusiveThread();
        }

        try {
            checkKryoInitialized();
            if (this.copyInstance == null) {
                this.copyInstance = createInstance();
            }

            T tmp = deserialize(copyInstance, source);
            serialize(tmp, target);
        } finally {
            if (CONCURRENT_ACCESS_CHECK) {
                exitExclusiveThread();
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (kryoRegistrations.hashCode());
        result = 31 * result + (defaultSerializers.hashCode());
        result = 31 * result + (defaultSerializerClasses.hashCode());

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof KryoSerializer) {
            KryoSerializer<?> other = (KryoSerializer<?>) obj;

            return type == other.type
                    && Objects.equals(kryoRegistrations, other.kryoRegistrations)
                    && Objects.equals(defaultSerializerClasses, other.defaultSerializerClasses)
                    && Objects.equals(defaultSerializers, other.defaultSerializers);
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns the Chill Kryo Serializer which is implicitly added to the classpath via
     * flink-runtime. Falls back to the default Kryo serializer if it can't be found.
     *
     * @return The Kryo serializer instance.
     */
    private Kryo getKryoInstance() {

        try {
            // check if ScalaKryoInstantiator is in class path (coming from Twitter's Chill
            // library).
            // This will be true if Flink's Scala API is used.
            Class<?> chillInstantiatorClazz =
                    Class.forName("org.apache.flink.runtime.types.FlinkScalaKryoInstantiator");
            Object chillInstantiator = chillInstantiatorClazz.newInstance();

            // obtain a Kryo instance through Twitter Chill
            Method m = chillInstantiatorClazz.getMethod("newKryo");

            return (Kryo) m.invoke(chillInstantiator);
        } catch (ClassNotFoundException
                | InstantiationException
                | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException e) {

            if (LOG.isDebugEnabled()) {
                LOG.info("Kryo serializer scala extensions are not available.", e);
            } else {
                LOG.info("Kryo serializer scala extensions are not available.");
            }

            Kryo.DefaultInstantiatorStrategy initStrategy = new Kryo.DefaultInstantiatorStrategy();
            initStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(initStrategy);

            if (flinkChillPackageRegistrar != null) {
                flinkChillPackageRegistrar.registerSerializers(kryo);
            }

            return kryo;
        }
    }

    private void checkKryoInitialized() {
        if (this.kryo == null) {
            this.kryo = getKryoInstance();

            // Enable reference tracking.
            kryo.setReferences(true);

            // Throwable and all subclasses should be serialized via java serialization
            // Note: the registered JavaSerializer is Flink's own implementation, and not Kryo's.
            //       This is due to a know issue with Kryo's JavaSerializer. See FLINK-6025 for
            // details.
            kryo.addDefaultSerializer(Throwable.class, new JavaSerializer());

            // Add default serializers first, so that the type registrations without a serializer
            // are registered with a default serializer
            for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>> entry :
                    defaultSerializers.entrySet()) {
                kryo.addDefaultSerializer(entry.getKey(), entry.getValue().getSerializer());
            }

            for (Map.Entry<Class<?>, Class<? extends Serializer<?>>> entry :
                    defaultSerializerClasses.entrySet()) {
                kryo.addDefaultSerializer(entry.getKey(), entry.getValue());
            }

            KryoUtils.applyRegistrations(
                    this.kryo,
                    kryoRegistrations.values(),
                    flinkChillPackageRegistrar != null
                            ? flinkChillPackageRegistrar.getNextRegistrationId()
                            : kryo.getNextRegistrationId());

            kryo.setRegistrationRequired(false);
            kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
        }
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new KryoSerializerSnapshot<>(
                type, defaultSerializers, defaultSerializerClasses, kryoRegistrations);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    /**
     * Utility method that takes lists of registered types and their serializers, and resolve them
     * into a single list such that the result will resemble the final registration result in Kryo.
     */
    private static LinkedHashMap<String, KryoRegistration> buildKryoRegistrations(
            Class<?> serializedType,
            LinkedHashSet<Class<?>> registeredTypes,
            LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>
                    registeredTypesWithSerializerClasses,
            LinkedHashMap<Class<?>, ExecutionConfig.SerializableSerializer<?>>
                    registeredTypesWithSerializers) {

        final LinkedHashMap<String, KryoRegistration> kryoRegistrations = new LinkedHashMap<>();

        kryoRegistrations.put(serializedType.getName(), new KryoRegistration(serializedType));

        for (Class<?> registeredType : checkNotNull(registeredTypes)) {
            kryoRegistrations.put(registeredType.getName(), new KryoRegistration(registeredType));
        }

        for (Map.Entry<Class<?>, Class<? extends Serializer<?>>>
                registeredTypeWithSerializerClassEntry :
                        checkNotNull(registeredTypesWithSerializerClasses).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerClassEntry.getKey().getName(),
                    new KryoRegistration(
                            registeredTypeWithSerializerClassEntry.getKey(),
                            registeredTypeWithSerializerClassEntry.getValue()));
        }

        for (Map.Entry<Class<?>, ExecutionConfig.SerializableSerializer<?>>
                registeredTypeWithSerializerEntry :
                        checkNotNull(registeredTypesWithSerializers).entrySet()) {

            kryoRegistrations.put(
                    registeredTypeWithSerializerEntry.getKey().getName(),
                    new KryoRegistration(
                            registeredTypeWithSerializerEntry.getKey(),
                            registeredTypeWithSerializerEntry.getValue()));
        }

        // add Avro support if flink-avro is available; a dummy otherwise
        AvroUtils.getAvroUtils().addAvroGenericDataArrayRegistration(kryoRegistrations);

        return kryoRegistrations;
    }

    static void configureKryoLogging() {
        // Kryo uses only DEBUG and TRACE levels
        // we only forward TRACE level, because even DEBUG levels results in
        // a logging for each object, which is infeasible in Flink.
        if (LOG.isTraceEnabled()) {
            com.esotericsoftware.minlog.Log.setLogger(new MinlogForwarder(LOG));
            com.esotericsoftware.minlog.Log.TRACE();
        }
    }

    // --------------------------------------------------------------------------------------------

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        // kryoRegistrations may be null if this Kryo serializer is deserialized from an old version
        if (kryoRegistrations == null) {
            this.kryoRegistrations =
                    buildKryoRegistrations(
                            type,
                            registeredTypes,
                            registeredTypesWithSerializerClasses,
                            registeredTypesWithSerializers);
        }
    }

    private ExecutionConfig.SerializableSerializer<? extends Serializer<?>> deepCopySerializer(
            ExecutionConfig.SerializableSerializer<? extends Serializer<?>> original) {
        try {
            return InstantiationUtil.clone(
                    original, Thread.currentThread().getContextClassLoader());
        } catch (IOException | ClassNotFoundException ex) {
            throw new CloneFailedException(
                    "Could not clone serializer instance of class " + original.getClass(), ex);
        }
    }

    // --------------------------------------------------------------------------------------------
    // For testing
    // --------------------------------------------------------------------------------------------

    private void enterExclusiveThread() {
        // we use simple get, check, set here, rather than CAS
        // we don't need lock-style correctness, this is only a sanity-check and we thus
        // favor speed at the cost of some false negatives in this check
        Thread previous = currentThread;
        Thread thisThread = Thread.currentThread();

        if (previous == null) {
            currentThread = thisThread;
        } else if (previous != thisThread) {
            throw new IllegalStateException(
                    "Concurrent access to KryoSerializer. Thread 1: "
                            + thisThread.getName()
                            + " , Thread 2: "
                            + previous.getName());
        }
    }

    private void exitExclusiveThread() {
        currentThread = null;
    }

    @VisibleForTesting
    public Kryo getKryo() {
        checkKryoInitialized();
        return this.kryo;
    }
}
