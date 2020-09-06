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

package org.apache.flink.runtime.rest.compatibility;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import org.junit.Assert;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Contains the compatibility checks that are applied by the {@link RestAPIStabilityTest}. New checks must be added to
 * the {@link CompatibilityRoutines#ROUTINES} collection.
 */
enum CompatibilityRoutines {
	;

	private static final CompatibilityRoutine<String> URL_ROUTINE = new CompatibilityRoutine<>(
		"url",
		String.class,
		RestHandlerSpecification::getTargetRestEndpointURL,
		Assert::assertEquals);

	private static final CompatibilityRoutine<String> METHOD_ROUTINE = new CompatibilityRoutine<>(
		"method",
		String.class,
		header -> header.getHttpMethod().getNettyHttpMethod().name(),
		Assert::assertEquals);

	private static final CompatibilityRoutine<String> STATUS_CODE_ROUTINE = new CompatibilityRoutine<>(
		"status-code",
		String.class,
		header -> header.getResponseStatusCode().toString(),
		Assert::assertEquals);

	private static final CompatibilityRoutine<Boolean> FILE_UPLOAD_ROUTINE = new CompatibilityRoutine<>(
		"file-upload",
		Boolean.class,
		UntypedResponseMessageHeaders::acceptsFileUploads,
		Assert::assertEquals);

	private static final CompatibilityRoutine<PathParameterContainer> PATH_PARAMETER_ROUTINE = new CompatibilityRoutine<>(
		"path-parameters",
		PathParameterContainer.class,
		header -> {
			List<PathParameterContainer.PathParameter> pathParameters = header.getUnresolvedMessageParameters().getPathParameters().stream()
				.map(param -> new PathParameterContainer.PathParameter(param.getKey()))
				.collect(Collectors.toList());
			return new PathParameterContainer(pathParameters);
		},
		CompatibilityRoutines::assertCompatible);

	private static final CompatibilityRoutine<QueryParameterContainer> QUERY_PARAMETER_ROUTINE = new CompatibilityRoutine<>(
		"query-parameters",
		QueryParameterContainer.class,
		header -> {
			List<QueryParameterContainer.QueryParameter> pathParameters = header.getUnresolvedMessageParameters().getQueryParameters().stream()
				.map(param -> new QueryParameterContainer.QueryParameter(param.getKey(), param.isMandatory()))
				.collect(Collectors.toList());
			return new QueryParameterContainer(pathParameters);
		},
		CompatibilityRoutines::assertCompatible);

	private static final CompatibilityRoutine<JsonNode> REQUEST_ROUTINE = new CompatibilityRoutine<>(
		"request",
		JsonNode.class,
		header -> extractSchema(header.getRequestClass()),
		CompatibilityRoutines::assertCompatible
	);

	private static final CompatibilityRoutine<JsonNode> RESPONSE_ROUTINE = new CompatibilityRoutine<>(
		"response",
		JsonNode.class,
		header -> extractSchema(header.getResponseClass()),
		CompatibilityRoutines::assertCompatible
	);

	static final Collection<CompatibilityRoutine<?>> ROUTINES = Collections.unmodifiableList(Arrays.asList(
		URL_ROUTINE,
		METHOD_ROUTINE,
		STATUS_CODE_ROUTINE,
		FILE_UPLOAD_ROUTINE,
		PATH_PARAMETER_ROUTINE,
		QUERY_PARAMETER_ROUTINE,
		REQUEST_ROUTINE,
		RESPONSE_ROUTINE
	));

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final JsonSchemaGenerator SCHEMA_GENERATOR = new JsonSchemaGenerator(OBJECT_MAPPER);

	private static void assertCompatible(final PathParameterContainer old, final PathParameterContainer cur) {
		for (final PathParameterContainer.PathParameter oldParam : old.pathParameters) {
			if (cur.pathParameters.stream().noneMatch(param -> param.key.equals(oldParam.key))) {
				Assert.fail(String.format(
					"Existing Path parameter %s was removed.",
					oldParam.key));
			}
		}
		// contrary to other routines path parameters must be completely identical between versions, so we have to
		// check both directions
		for (final PathParameterContainer.PathParameter curParam : cur.pathParameters) {
			if (old.pathParameters.stream().noneMatch(param -> param.key.equals(curParam.key))) {
				Assert.fail(String.format(
					"New path parameter %s was added.",
					curParam.key));
			}
		}
	}

	private static void assertCompatible(final QueryParameterContainer old, final QueryParameterContainer cur) {
		for (final QueryParameterContainer.QueryParameter oldParam : old.queryParameters) {
			final Optional<QueryParameterContainer.QueryParameter> matchingParameter = cur.queryParameters.stream()
				.filter(param -> param.key.equals(oldParam.key))
				.findAny();

			if (matchingParameter.isPresent()) {
				final QueryParameterContainer.QueryParameter newParam = matchingParameter.get();
				if (!oldParam.mandatory && newParam.mandatory) {
					Assert.fail(String.format(
						"Previously optional query parameter %s is now mandatory.",
						oldParam.key));
				}
			} else {
				Assert.fail(String.format(
					"Query parameter %s was removed.",
					oldParam.key));
			}
		}
	}

	private static JsonNode extractSchema(final Class<?> messageClass) {
		try {
			final JsonSchema schema = SCHEMA_GENERATOR.generateSchema(messageClass);

			return OBJECT_MAPPER.valueToTree(schema);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to generate message schema for class " + messageClass.getCanonicalName() + '.', e);
		}
	}

	private static void assertCompatible(final JsonNode old, final JsonNode cur) {
		final Deque<Tuple2<JsonNode, JsonNode>> stack = new ArrayDeque<>(8);
		stack.add(Tuple2.of(old, cur));

		while (!stack.isEmpty()) {
			final Tuple2<JsonNode, JsonNode> propertyPair = stack.pop();

			final JsonNode oldProperty = propertyPair.f0;
			final JsonNode curProperty = propertyPair.f1;

			Assert.assertNotNull("Field " + oldProperty + " was removed.", curProperty);

			final String oldType = oldProperty.get("type").asText();
			final String curType = curProperty.get("type").asText();

			// 'any' as a type only occurs for properties where we have custom serialization code
			// removing this custom code (and thus improving the schema) should be possible
			// hence we only check equality for other types
			if (!oldType.equals("any")) {
				Assert.assertEquals(
					String.format("Type of field was changed from '%s' to '%s'.", oldType, curType),
					oldType, curType);
			}

			if (oldType.equals("array")) {
				final JsonNode oldArrayItems = oldProperty.get("items");
				final JsonNode curArrayItems = curProperty.get("items");

				stack.addLast(Tuple2.of(
					oldArrayItems,
					curArrayItems));
			} else if (oldType.equals("object")) {
				final JsonNode oldProperties = oldProperty.get("properties");
				final JsonNode curProperties = curProperty.get("properties");

				if (oldProperties != null) {
					for (Iterator<Map.Entry<String, JsonNode>> it = oldProperties.fields(); it.hasNext(); ) {
						final Map.Entry<String, JsonNode> oldPropertyWithKey = it.next();
						stack.addLast(Tuple2.of(
							oldPropertyWithKey.getValue(),
							curProperties.get(oldPropertyWithKey.getKey())));
					}
				}
			} // else assume basic types
		}
	}

	static final class PathParameterContainer {
		public Collection<PathParameterContainer.PathParameter> pathParameters;

		private PathParameterContainer() {
			// required by jackson
		}

		PathParameterContainer(Collection<PathParameterContainer.PathParameter> pathParameters) {
			this.pathParameters = pathParameters;
		}

		static final class PathParameter {
			public String key;

			private PathParameter() {
				// required by jackson
			}

			PathParameter(String key) {
				this.key = key;
			}
		}
	}

	static final class QueryParameterContainer {
		public Collection<QueryParameterContainer.QueryParameter> queryParameters;

		private QueryParameterContainer() {
			// required by jackson
		}

		QueryParameterContainer(Collection<QueryParameterContainer.QueryParameter> queryParameters) {
			this.queryParameters = queryParameters;
		}

		static final class QueryParameter {
			public String key;
			public boolean mandatory;

			private QueryParameter() {
			}

			QueryParameter(String key, boolean mandatory) {
				this.key = key;
				this.mandatory = mandatory;
			}
		}
	}
}
