/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The deserialization schema describes how to turn the byte messages delivered by certain
 * data sources (for example Apache Kafka) into data types (Java/Scala objects) that are
 * processed by Flink.
 *
 * <p>This base variant of the deserialization schema produces the type information
 * automatically by extracting it from the generic class arguments.
 *
 * <h3>Common Use</h3>
 *
 * <p>To write a deserialization schema for a specific type, simply extend this class and declare
 * the type in the class signature. Flink will reflectively determine the type and create the
 * proper TypeInformation:
 *
 * <pre>{@code
 * public class MyDeserializationSchema extends AbstractDeserializationSchema<MyType> {
 *
 *     public MyType deserialize(byte[] message) throws IOException {
 *         ...
 *     }
 * }
 * }</pre>
 *
 * <h3>Generic Use</h3>
 *
 * <p>If you want to write a more generic DeserializationSchema that works for different types,
 * you need to pass the TypeInformation (or an equivalent hint) to the constructor:
 *
 * <pre>{@code
 * public class MyGenericSchema<T> extends AbstractDeserializationSchema<T> {
 *
 *     public MyGenericSchema(Class<T> type) {
 *         super(type);
 *     }
 *
 *     public T deserialize(byte[] message) throws IOException {
 *         ...
 *     }
 * }
 * }</pre>
 *
 * @param <T> The type created by the deserialization schema.
 */
@PublicEvolving
public abstract class AbstractDeserializationSchema<T> implements DeserializationSchema<T> {

	private static final long serialVersionUID = 2L;

	/** The type produced by this {@code DeserializationSchema}. */
	private final TypeInformation<T> type;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new AbstractDeserializationSchema and tries to infer the type returned by this
	 * DeserializationSchema.
	 *
	 * <p>This constructor is usable whenever the DeserializationSchema concretely defines
	 * its type, without generic variables:
	 *
	 * <pre>{@code
	 * public class MyDeserializationSchema extends AbstractDeserializationSchema<MyType> {
	 *
	 *     public MyType deserialize(byte[] message) throws IOException {
	 *         ...
	 *     }
	 * }
	 * }</pre>
	 */
	protected AbstractDeserializationSchema() {
		try {
			this.type = TypeExtractor.createTypeInfo(
					AbstractDeserializationSchema.class, getClass(), 0, null, null);
		}
		catch (InvalidTypesException e) {
			throw new FlinkRuntimeException(
					"The implementation of AbstractDeserializationSchema is using a generic variable. " +
					"This is not supported, because due to Java's generic type erasure, it will not be possible to " +
					"determine the full type at runtime. For generic implementations, please pass the TypeInformation " +
					"or type class explicitly to the constructor.");
		}
	}

	/**
	 * Creates an AbstractDeserializationSchema that returns the TypeInformation
	 * indicated by the given class. This constructor is only necessary when creating a generic
	 * implementation, see {@link AbstractDeserializationSchema Generic Use}.
	 *
	 * <p>This constructor may fail if the class is generic. In that case, please
	 * use the constructor that accepts a {@link #AbstractDeserializationSchema(TypeHint) TypeHint},
	 * or a {@link #AbstractDeserializationSchema(TypeInformation) TypeInformation}.
	 *
	 * @param type The class of the produced type.
	 */
	protected AbstractDeserializationSchema(Class<T> type) {
		checkNotNull(type, "type");
		this.type = TypeInformation.of(type);
	}

	/**
	 * Creates an AbstractDeserializationSchema that returns the TypeInformation
	 * indicated by the given type hint. This constructor is only necessary when creating a generic
	 * implementation, see {@link AbstractDeserializationSchema Generic Use}.
	 *
	 * @param typeHint The TypeHint for the produced type.
	 */
	protected AbstractDeserializationSchema(TypeHint<T> typeHint) {
		checkNotNull(typeHint, "typeHint");
		this.type = typeHint.getTypeInfo();
	}

	/**
	 * Creates an AbstractDeserializationSchema that returns the given TypeInformation
	 * for the produced type. This constructor is only necessary when creating a generic
	 * implementation, see {@link AbstractDeserializationSchema Generic Use}.
	 *
	 * @param typeInfo The TypeInformation for the produced type.
	 */
	protected AbstractDeserializationSchema(TypeInformation<T> typeInfo) {
		this.type = checkNotNull(typeInfo, "typeInfo");
	}

	// ------------------------------------------------------------------------

	/**
	 * De-serializes the byte message.
	 *
	 * @param message The message, as a byte array.
	 * @return The de-serialized message as an object.
	 */
	@Override
	public abstract T deserialize(byte[] message) throws IOException;

	/**
	 * Method to decide whether the element signals the end of the stream. If
	 * true is returned the element won't be emitted.
	 *
	 * <p>This default implementation returns always false, meaning the stream is interpreted
	 * to be unbounded.
	 *
	 * @param nextElement The element to test for the end-of-stream signal.
	 * @return True, if the element signals end of stream, false otherwise.
	 */
	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}

	/**
	 * Gets the type produced by this deserializer.
	 * This is the type that was passed to the  constructor, or reflectively inferred
	 * (if the default constructor was called).
	 */
	@Override
	public TypeInformation<T> getProducedType() {
		return type;
	}
}
