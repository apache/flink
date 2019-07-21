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

package org.apache.flink.formats.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.InstantiationUtil.deserializeObject;
import static org.apache.flink.util.InstantiationUtil.serializeObject;

/**
 * Matcher that provides a common way for asserting results of {@link SerializationSchema}. It takes into account
 * e.g. the fact that serialization schema during runtime might be used after serializing and deserializing it over
 * a wire. Usage:
 *
 * <ul>
 * <li>when asserting for result after serializing and deserializing a row
 * <pre>{@code
 *      final JsonRowSerializationSchema serializationSchema = new JsonRowSerializationSchema(rowSchema);
 *      final JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(rowSchema);
 *
 *      assertThat(row, whenSerializedWith(serializationSchema)
 *          .andDeserializedWith(deserializationSchema)
 *          .matches(row));
 * }</pre>
 * </li>
 *
 * <li>to check if an exception is thrown during serialization:
 * <pre>{@code
 *      assertThat(row, whenSerializedWith(serializationSchema).failsWithException(instanceOf(RuntimeException.class)));
 * }</pre>
 * </li>
 * </ul>
 */
public abstract class SerializationSchemaMatcher extends TypeSafeMatcher<Row> {

	final SerializationSchema<Row> serializationSchema;

	private SerializationSchemaMatcher(SerializationSchema<Row> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	public static SerializationSchemaMatcherBuilder whenSerializedWith(SerializationSchema<Row> serializationSchema) {
		return new SerializationSchemaMatcherBuilder(serializationSchema);
	}

	private static class SerializationSchemaResultMatcher extends SerializationSchemaMatcher {

		private final Row expected;
		private final DeserializationSchema<Row> deserializationSchema;

		private SerializationSchemaResultMatcher(
				SerializationSchema<Row> serializationSchema,
				DeserializationSchema<Row> deserializationSchema,
				Row expected) {
			super(serializationSchema);

			this.expected = expected;
			this.deserializationSchema = deserializationSchema;
		}

		@Override
		protected boolean matchesSafely(Row item) {
			try {
				return Objects.deepEquals(
					deserializationSchema.deserialize(serializationSchema.serialize(item)),
					expected);
			} catch (IOException e) {
				throw new AssertionError("Could not deserialize", e);
			}
		}

		@Override
		public void describeTo(Description description) {
			description.appendValue(expected);
		}
	}

	private static class SerializationSchemaExceptionMatcher extends SerializationSchemaMatcher {

		private final Matcher<? extends Throwable> exceptionMatcher;
		private Throwable thrownException = null;

		private SerializationSchemaExceptionMatcher(
				SerializationSchema<Row> serializationSchema,
				Matcher<? extends Throwable> exceptionMatcher) {
			super(serializationSchema);
			this.exceptionMatcher = exceptionMatcher;
		}

		@Override
		protected boolean matchesSafely(Row item) {
			try {
				serializationSchema.serialize(item);
			} catch (Exception e) {
				thrownException = e;
			}
			return exceptionMatcher.matches(thrownException);
		}

		@Override
		public void describeTo(Description description) {
			exceptionMatcher.describeTo(description);
		}

		@Override
		protected void describeMismatchSafely(Row item, Description mismatchDescription) {
			exceptionMatcher.describeMismatch(thrownException, mismatchDescription);
		}
	}

	/**
	 * Builder for {@link SerializationSchemaMatcher} that can assert results after serialize and deserialize.
	 */
	public static class SerializationWithDeserializationSchemaMatcherBuilder {

		private SerializationSchema<Row> serializationSchema;
		private DeserializationSchema<Row> deserializationSchema;

		private SerializationWithDeserializationSchemaMatcherBuilder(
			SerializationSchema<Row> serializationSchema,
			DeserializationSchema<Row> deserializationSchema) {
			try {
				// we serialize and deserialize the schema to test runtime behavior
				// when the schema is shipped to the cluster
				this.serializationSchema = deserializeObject(
					serializeObject(serializationSchema),
					this.getClass().getClassLoader());
				this.deserializationSchema = deserializeObject(
					serializeObject(deserializationSchema),
					this.getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		public SerializationSchemaMatcher equalsTo(Row expected) {
			return new SerializationSchemaResultMatcher(
				serializationSchema,
				deserializationSchema,
				expected
			);
		}
	}

	/**
	 * Builder for {@link SerializationSchemaMatcher}.
	 */
	public static class SerializationSchemaMatcherBuilder {

		private SerializationSchema<Row> serializationSchema;

		private SerializationSchemaMatcherBuilder(SerializationSchema<Row> serializationSchema) {
			this.serializationSchema = serializationSchema;
		}

		public SerializationWithDeserializationSchemaMatcherBuilder andDeserializedWith(DeserializationSchema<Row> deserializationSchema) {
			return new SerializationWithDeserializationSchemaMatcherBuilder(serializationSchema, deserializationSchema);
		}

		public SerializationSchemaMatcher failsWithException(Matcher<? extends Throwable> exceptionMatcher) {
			return new SerializationSchemaExceptionMatcher(
				serializationSchema,
				exceptionMatcher
			);
		}
	}
}
