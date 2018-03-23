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

package org.apache.flink.runtime.rest.messages.json;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonToken;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.TypeFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * JSON deserializer for {@link JobResult}.
 *
 * @see JobResultSerializer
 */
public class JobResultDeserializer extends StdDeserializer<JobResult> {

	private static final long serialVersionUID = 1L;

	private final JobIDDeserializer jobIdDeserializer = new JobIDDeserializer();

	private final SerializedThrowableDeserializer serializedThrowableDeserializer =
		new SerializedThrowableDeserializer();

	private final SerializedValueDeserializer serializedValueDeserializer;

	public JobResultDeserializer() {
		super(JobResult.class);
		final JavaType objectSerializedValueType = TypeFactory.defaultInstance()
			.constructType(new TypeReference<SerializedValue<Object>>() {
			});
		serializedValueDeserializer = new SerializedValueDeserializer(objectSerializedValueType);
	}

	@Override
	public JobResult deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
		JobID jobId = null;
		long netRuntime = -1;
		SerializedThrowable serializedThrowable = null;
		Map<String, SerializedValue<OptionalFailure<Object>>> accumulatorResults = null;

		while (true) {
			final JsonToken jsonToken = p.nextToken();
			assertNotEndOfInput(p, jsonToken);
			if (jsonToken == JsonToken.END_OBJECT) {
				break;
			}

			final String fieldName = p.getValueAsString();
			switch (fieldName) {
				case JobResultSerializer.FIELD_NAME_JOB_ID:
					assertNextToken(p, JsonToken.VALUE_STRING);
					jobId = jobIdDeserializer.deserialize(p, ctxt);
					break;
				case JobResultSerializer.FIELD_NAME_NET_RUNTIME:
					assertNextToken(p, JsonToken.VALUE_NUMBER_INT);
					netRuntime = p.getLongValue();
					break;
				case JobResultSerializer.FIELD_NAME_ACCUMULATOR_RESULTS:
					assertNextToken(p, JsonToken.START_OBJECT);
					accumulatorResults = parseAccumulatorResults(p, ctxt);
					break;
				case JobResultSerializer.FIELD_NAME_FAILURE_CAUSE:
					assertNextToken(p, JsonToken.START_OBJECT);
					serializedThrowable = serializedThrowableDeserializer.deserialize(p, ctxt);
					break;
				default:
					// ignore unknown fields
			}
		}

		try {
			return new JobResult.Builder()
				.jobId(jobId)
				.netRuntime(netRuntime)
				.accumulatorResults(accumulatorResults)
				.serializedThrowable(serializedThrowable)
				.build();
		} catch (final RuntimeException e) {
			throw new JsonMappingException(
				null,
				"Could not deserialize " + JobResult.class.getSimpleName(),
				e);
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, SerializedValue<OptionalFailure<Object>>> parseAccumulatorResults(
			final JsonParser p,
			final DeserializationContext ctxt) throws IOException {

		final Map<String, SerializedValue<OptionalFailure<Object>>> accumulatorResults = new HashMap<>();
		while (true) {
			final JsonToken jsonToken = p.nextToken();
			assertNotEndOfInput(p, jsonToken);
			if (jsonToken == JsonToken.END_OBJECT) {
				break;
			}
			final String accumulatorName = p.getValueAsString();
			p.nextValue();
			accumulatorResults.put(
				accumulatorName,
				(SerializedValue<OptionalFailure<Object>>) serializedValueDeserializer.deserialize(p, ctxt));
		}
		return accumulatorResults;
	}

	/**
	 * Asserts that the provided JsonToken is not null, i.e., not at the end of the input.
	 */
	private static void assertNotEndOfInput(
			final JsonParser p,
			@Nullable final JsonToken jsonToken) {
		checkState(jsonToken != null, "Unexpected end of input at %s", p.getCurrentLocation());
	}

	/**
	 * Advances the token and asserts that it matches the required {@link JsonToken}.
	 */
	private static void assertNextToken(
			final JsonParser p,
			final JsonToken requiredJsonToken) throws IOException {
		final JsonToken jsonToken = p.nextToken();
		if (jsonToken != requiredJsonToken) {
			throw new JsonMappingException(p, String.format("Expected token %s (was %s)", requiredJsonToken, jsonToken));
		}
	}
}
