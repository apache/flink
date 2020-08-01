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
import org.apache.flink.types.Row;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.InstantiationUtil.deserializeObject;
import static org.apache.flink.util.InstantiationUtil.serializeObject;

/**
 * Matcher that provides a common way for asserting results of {@link DeserializationSchema}. It takes into account
 * e.g. the fact that serialization schema during runtime might be used after serializing it over a wire. Usage:
 *
 * <ul>
 * <li>when asserting for result after deserializing a row
 * <pre>{@code
 *      byte[] jsonBytes = ...
 *      Row expectedRow = ...
 *      final JsonRowDeserializationSchema deserializationSchema = new JsonRowDeserializationSchema(rowSchema);
 *
 *      assertThat(jsonBytes, whenDeserializedWith(deserializationSchema)
 *          .matches(expectedRow));
 * }</pre>
 * </li>
 *
 * <li>to check if an exception is thrown during serialization:
 * <pre>{@code
 *      assertThat(serializedJson,
 *          whenDeserializedWith(deserializationSchema)
 *              .failsWithException(hasCause(instanceOf(IllegalStateException.class))));
 * }</pre>
 * </li>
 * </ul>
 */
public abstract class DeserializationSchemaMatcher extends TypeSafeMatcher<byte[]> {

	final DeserializationSchema<Row> deserializationSchema;

	private DeserializationSchemaMatcher(DeserializationSchema<Row> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	public static DeserializationSchemaMatcherBuilder whenDeserializedWith(DeserializationSchema<Row> deserializationSchema) {
		return new DeserializationSchemaMatcherBuilder(deserializationSchema);
	}

	private static class DeserializationSchemaResultMatcher extends DeserializationSchemaMatcher {

		private final Row expected;

		private DeserializationSchemaResultMatcher(
			DeserializationSchema<Row> deserializationSchema,
			Row expected) {
			super(deserializationSchema);

			this.expected = expected;
		}

		@Override
		protected boolean matchesSafely(byte[] item) {
			try {
				return Objects.deepEquals(deserializationSchema.deserialize(item), expected);
			} catch (IOException e) {
				throw new AssertionError("Could not deserialize", e);
			}
		}

		@Override
		public void describeTo(Description description) {
			description.appendValue(expected);
		}
	}

	private static class DeserializationSchemaExceptionMatcher extends DeserializationSchemaMatcher {

		private final Matcher<? extends Throwable> exceptionMatcher;
		private Throwable thrownException = null;

		private DeserializationSchemaExceptionMatcher(
			DeserializationSchema<Row> deserializationSchema,
			Matcher<? extends Throwable> exceptionMatcher) {
			super(deserializationSchema);
			this.exceptionMatcher = exceptionMatcher;
		}

		@Override
		protected boolean matchesSafely(byte[] item) {
			try {
				deserializationSchema.deserialize(item);
			} catch (IOException e) {
				thrownException = e;
			}
			return exceptionMatcher.matches(thrownException);
		}

		@Override
		public void describeTo(Description description) {
			exceptionMatcher.describeTo(description);
		}

		@Override
		protected void describeMismatchSafely(byte[] item, Description mismatchDescription) {
			exceptionMatcher.describeMismatch(thrownException, mismatchDescription);
		}
	}

	/**
	 * Builder for {@link DeserializationSchemaMatcher}.
	 */
	public static class DeserializationSchemaMatcherBuilder {

		private DeserializationSchema<Row> deserializationSchema;

		private DeserializationSchemaMatcherBuilder(DeserializationSchema<Row> deserializationSchema) {
			try {
				// we serialize and deserialize the schema to test runtime behavior
				// when the schema is shipped to the cluster
				this.deserializationSchema = deserializeObject(
					serializeObject(deserializationSchema),
					this.getClass().getClassLoader());
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		public DeserializationSchemaMatcher equalsTo(Row row) {
			return new DeserializationSchemaResultMatcher(
				deserializationSchema,
				row
			);
		}

		public DeserializationSchemaMatcher failsWithException(Matcher<? extends Throwable> exceptionMatcher) {
			return new DeserializationSchemaExceptionMatcher(
				deserializationSchema,
				exceptionMatcher
			);
		}
	}
}
