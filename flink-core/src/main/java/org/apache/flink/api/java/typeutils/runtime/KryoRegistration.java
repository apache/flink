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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.util.Preconditions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.factories.ReflectionSerializerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;

/** A {@code KryoRegistration} resembles a registered class and its serializer in Kryo. */
@Internal
public class KryoRegistration implements Serializable {

    private static final long serialVersionUID = 5375110512910892655L;

    /**
     * IMPORTANT: the order of the enumerations must not change, since their ordinals are used for
     * serialization.
     */
    public enum SerializerDefinitionType {
        UNSPECIFIED,
        CLASS,
        INSTANCE
    }

    /**
     * The registered class.
     *
     * <p>This can be a dummy class {@link
     * KryoRegistrationSerializerConfigSnapshot.DummyRegisteredClass} if the class no longer exists
     * when this registration instance was restored.
     */
    private final Class<?> registeredClass;

    /**
     * Class of the serializer to use for the registered class. Exists only if the serializer
     * definition type is {@link SerializerDefinitionType#CLASS}.
     *
     * <p>This can be a dummy serializer {@link
     * KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass} if the serializer class no
     * longer exists when this registration instance was restored.
     */
    @Nullable private final Class<? extends Serializer<?>> serializerClass;

    /**
     * A serializable instance of the serializer to use for the registered class. Exists only if the
     * serializer definition type is {@link SerializerDefinitionType#INSTANCE}.
     *
     * <p>This can be a dummy serializer {@link
     * KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass} if the serializer class no
     * longer exists or is no longer valid when this registration instance was restored.
     */
    @Nullable
    private final ExecutionConfig.SerializableSerializer<? extends Serializer<?>>
            serializableSerializerInstance;

    private final SerializerDefinitionType serializerDefinitionType;

    public KryoRegistration(Class<?> registeredClass) {
        this.registeredClass = Preconditions.checkNotNull(registeredClass);

        this.serializerClass = null;
        this.serializableSerializerInstance = null;

        this.serializerDefinitionType = SerializerDefinitionType.UNSPECIFIED;
    }

    public KryoRegistration(
            Class<?> registeredClass, Class<? extends Serializer<?>> serializerClass) {
        this.registeredClass = Preconditions.checkNotNull(registeredClass);

        this.serializerClass = Preconditions.checkNotNull(serializerClass);
        this.serializableSerializerInstance = null;

        this.serializerDefinitionType = SerializerDefinitionType.CLASS;
    }

    public KryoRegistration(
            Class<?> registeredClass,
            ExecutionConfig.SerializableSerializer<? extends Serializer<?>>
                    serializableSerializerInstance) {
        this.registeredClass = Preconditions.checkNotNull(registeredClass);

        this.serializerClass = null;
        this.serializableSerializerInstance =
                Preconditions.checkNotNull(serializableSerializerInstance);

        this.serializerDefinitionType = SerializerDefinitionType.INSTANCE;
    }

    public Class<?> getRegisteredClass() {
        return registeredClass;
    }

    public SerializerDefinitionType getSerializerDefinitionType() {
        return serializerDefinitionType;
    }

    @Nullable
    public Class<? extends Serializer<?>> getSerializerClass() {
        return serializerClass;
    }

    @Nullable
    public ExecutionConfig.SerializableSerializer<? extends Serializer<?>>
            getSerializableSerializerInstance() {
        return serializableSerializerInstance;
    }

    public Serializer<?> getSerializer(Kryo kryo) {
        switch (serializerDefinitionType) {
            case UNSPECIFIED:
                return null;
            case CLASS:
                return ReflectionSerializerFactory.makeSerializer(
                        kryo, serializerClass, registeredClass);
            case INSTANCE:
                return serializableSerializerInstance.getSerializer();
            default:
                // this should not happen; adding as a guard for the future
                throw new IllegalStateException(
                        "Unrecognized Kryo registration serializer definition type: "
                                + serializerDefinitionType);
        }
    }

    public boolean isDummy() {
        return registeredClass.equals(
                        KryoRegistrationSerializerConfigSnapshot.DummyRegisteredClass.class)
                || (serializerClass != null
                        && serializerClass.equals(
                                KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass
                                        .class))
                || (serializableSerializerInstance != null
                        && serializableSerializerInstance.getSerializer()
                                instanceof
                                KryoRegistrationSerializerConfigSnapshot.DummyKryoSerializerClass);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj instanceof KryoRegistration) {
            KryoRegistration other = (KryoRegistration) obj;

            // we cannot include the serializer instances here because they don't implement the
            // equals method
            return serializerDefinitionType == other.serializerDefinitionType
                    && registeredClass == other.registeredClass
                    && serializerClass == other.serializerClass;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = serializerDefinitionType.hashCode();
        result = 31 * result + registeredClass.hashCode();

        if (serializerClass != null) {
            result = 31 * result + serializerClass.hashCode();
        }

        return result;
    }
}
