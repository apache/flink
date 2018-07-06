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

package org.apache.flink.docs.rest;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherRestEndpoint;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerConfiguration;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.SerializableString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.CharacterEscapes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.SerializedString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static org.apache.flink.docs.util.Utils.escapeCharacters;

/**
 * Generator for the Rest API documentation.
 *
 * <p>One HTML file is generated for each {@link RestServerEndpoint} implementation
 * that can be embedded into .md files using {@code {% include ${generated.docs.dir}/file.html %}}.
 * Each file contains a series of HTML tables, one for each REST call.
 *
 * <p>The generated table for each REST call looks like this:
 * ----------------------------------------------------------
 * | URL                                                    |
 * ----------------------------------------------------------
 * | Verb: verb (GET|POST|...) | Response code: responseCode|
 * ----------------------------------------------------------
 * | Path parameters (if any are defined)                   |
 * ----------------------------------------------------------
 * |   - parameterName: description                         |
 * |   ...                                                  |
 * ----------------------------------------------------------
 * | Query parameters (if any are defined)                  |
 * ----------------------------------------------------------
 * |   - parameterName (requisiteness): description         |
 * |   ...                                                  |
 * ----------------------------------------------------------
 * | Request json schema (a collapsible "Request" button)   |
 * ----------------------------------------------------------
 * | Response json schema (a collapsible "Response" button) |
 * ----------------------------------------------------------
 */
public class RestAPIDocGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(RestAPIDocGenerator.class);

	private static final ObjectMapper mapper;
	private static final JsonSchemaGenerator schemaGen;

	static {
		mapper = new ObjectMapper();
		mapper.getFactory().setCharacterEscapes(new HTMLCharacterEscapes());
		schemaGen = new JsonSchemaGenerator(mapper);
	}

	/**
	 * Generates the REST API documentation.
	 *
	 * @param args args[0] contains the directory into which the generated files are placed
	 * @throws IOException if any file operation failed
	 */
	public static void main(String[] args) throws IOException {
		String outputDirectory = args[0];

		createHtmlFile(new DocumentingDispatcherRestEndpoint(), Paths.get(outputDirectory, "rest_dispatcher.html"));
	}

	private static void createHtmlFile(DocumentingRestEndpoint restEndpoint, Path outputFile) throws IOException {
		StringBuilder html = new StringBuilder();

		List<MessageHeaders> specs = restEndpoint.getSpecs();
		specs.forEach(spec -> html.append(createHtmlEntry(spec)));

		Files.deleteIfExists(outputFile);
		Files.write(outputFile, html.toString().getBytes(StandardCharsets.UTF_8));
	}

	private static String createHtmlEntry(MessageHeaders<?, ?, ?> spec) {
		String requestEntry = createMessageHtmlEntry(
			spec.getRequestClass(),
			EmptyRequestBody.class);
		String responseEntry = createMessageHtmlEntry(
			spec.getResponseClass(),
			EmptyResponseBody.class);

		String pathParameterList = createPathParameterHtmlList(spec.getUnresolvedMessageParameters().getPathParameters());
		String queryParameterList = createQueryParameterHtmlList(spec.getUnresolvedMessageParameters().getQueryParameters());

		StringBuilder sb = new StringBuilder();
		{
			sb.append("<table class=\"table table-bordered\">\n");
			sb.append("  <tbody>\n");
			sb.append("    <tr>\n");
			sb.append("      <td class=\"text-left\" colspan=\"2\"><strong>" + spec.getTargetRestEndpointURL() + "</strong></td>\n");
			sb.append("    </tr>\n");
			sb.append("    <tr>\n");
			sb.append("      <td class=\"text-left\" style=\"width: 20%\">Verb: <code>" + spec.getHttpMethod() + "</code></td>\n");
			sb.append("      <td class=\"text-left\">Response code: <code>" + spec.getResponseStatusCode() + "</code></td>\n");
			sb.append("    </tr>\n");
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">" + escapeCharacters(spec.getDescription()) + "</td>\n");
			sb.append("    </tr>\n");
		}
		if (!pathParameterList.isEmpty()) {
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">Path parameters</td>\n");
			sb.append("    </tr>\n");
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">\n");
			sb.append("        <ul>\n");
			sb.append(pathParameterList);
			sb.append("        </ul>\n");
			sb.append("      </td>\n");
			sb.append("    </tr>\n");
		}
		if (!queryParameterList.isEmpty()) {
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">Query parameters</td>\n");
			sb.append("    </tr>\n");
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">\n");
			sb.append("        <ul>\n");
			sb.append(queryParameterList);
			sb.append("        </ul>\n");
			sb.append("      </td>\n");
			sb.append("    </tr>\n");
		}
		int reqHash = spec.getTargetRestEndpointURL().hashCode() + spec.getHttpMethod().name().hashCode() + spec.getRequestClass().getCanonicalName().hashCode();
		int resHash = spec.getTargetRestEndpointURL().hashCode() + spec.getHttpMethod().name().hashCode() + spec.getResponseClass().getCanonicalName().hashCode();
		{
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">\n");
			sb.append("        <button data-toggle=\"collapse\" data-target=\"#" + reqHash + "\">Request</button>\n");
			sb.append("        <div id=\"" + reqHash + "\" class=\"collapse\">\n");
			sb.append("          <pre>\n");
			sb.append("            <code>\n");
			sb.append(requestEntry);
			sb.append("            </code>\n");
			sb.append("          </pre>\n");
			sb.append("         </div>\n");
			sb.append("      </td>\n");
			sb.append("    </tr>\n");
			sb.append("    <tr>\n");
			sb.append("      <td colspan=\"2\">\n");
			sb.append("        <button data-toggle=\"collapse\" data-target=\"#" + resHash + "\">Response</button>\n");
			sb.append("        <div id=\"" + resHash + "\" class=\"collapse\">\n");
			sb.append("          <pre>\n");
			sb.append("            <code>\n");
			sb.append(responseEntry);
			sb.append("            </code>\n");
			sb.append("          </pre>\n");
			sb.append("         </div>\n");
			sb.append("      </td>\n");
			sb.append("    </tr>\n");
			sb.append("  </tbody>\n");
			sb.append("</table>\n");
		}

		return sb.toString();
	}

	private static String createPathParameterHtmlList(Collection<MessagePathParameter<?>> pathParameters) {
		StringBuilder pathParameterList = new StringBuilder();
		pathParameters.forEach(messagePathParameter ->
			pathParameterList.append(
				String.format("<li><code>%s</code> - %s</li>\n",
					messagePathParameter.getKey(),
					"description")
			));
		return pathParameterList.toString();
	}

	private static String createQueryParameterHtmlList(Collection<MessageQueryParameter<?>> queryParameters) {
		StringBuilder queryParameterList = new StringBuilder();
		queryParameters.stream()
			.sorted((param1, param2) -> Boolean.compare(param1.isMandatory(), param2.isMandatory()))
			.forEach(parameter ->
				queryParameterList.append(
					String.format("<li><code>%s</code> (%s): %s</li>\n",
						parameter.getKey(),
						parameter.isMandatory() ? "mandatory" : "optional",
						"description")
				));
		return queryParameterList.toString();
	}

	private static String createMessageHtmlEntry(Class<?> messageClass, Class<?> emptyMessageClass) {
		JsonSchema schema;
		try {
			schema = schemaGen.generateSchema(messageClass);
		} catch (JsonProcessingException e) {
			LOG.error("Failed to generate message schema for class {}.", messageClass, e);
			throw new RuntimeException("Failed to generate message schema for class " + messageClass.getCanonicalName() + ".", e);
		}

		String json;
		if (messageClass == emptyMessageClass) {
			json = "{}";
		} else {
			try {
				json = mapper.writerWithDefaultPrettyPrinter()
					.writeValueAsString(schema);
			} catch (JsonProcessingException e) {
				LOG.error("Failed to write message schema for class {}.", messageClass.getCanonicalName(), e);
				throw new RuntimeException("Failed to write message schema for class " + messageClass.getCanonicalName() + ".", e);
			}
		}

		return json;
	}

	/**
	 * Create character escapes for HTML when generating JSON request/response string.
	 *
	 * <p>This is to avoid exception when generating JSON with Field schema contains generic types.
	 */
	private static class HTMLCharacterEscapes extends CharacterEscapes {
		private final int[] asciiEscapes;
		private final Map<Integer, SerializableString> escapeSequences;

		public HTMLCharacterEscapes() {
			int[] esc = CharacterEscapes.standardAsciiEscapesForJSON();
			esc['<'] = CharacterEscapes.ESCAPE_CUSTOM;
			esc['>'] = CharacterEscapes.ESCAPE_CUSTOM;
			esc['&'] = CharacterEscapes.ESCAPE_CUSTOM;
			Map<Integer, SerializableString> escMap = new HashMap<>();
			escMap.put((int) '<', new SerializedString("&lt;"));
			escMap.put((int) '>', new SerializedString("&gt;"));
			escMap.put((int) '&', new SerializedString("&amp;"));
			asciiEscapes = esc;
			escapeSequences = escMap;
		}

		@Override
		public int[] getEscapeCodesForAscii() {
			return asciiEscapes;
		}

		@Override
		public SerializableString getEscapeSequence(int i) {
			return escapeSequences.getOrDefault(i, null);
		}
	}

	/**
	 * Utility class to extract the {@link MessageHeaders} that the {@link DispatcherRestEndpoint} supports.
	 */
	private static class DocumentingDispatcherRestEndpoint extends DispatcherRestEndpoint implements DocumentingRestEndpoint {

		private static final Configuration config;
		private static final RestServerEndpointConfiguration restConfig;
		private static final RestHandlerConfiguration handlerConfig;
		private static final Executor executor;
		private static final GatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever;
		private static final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
		private static final MetricQueryServiceRetriever metricQueryServiceRetriever;

		static {
			config = new Configuration();
			config.setString(RestOptions.ADDRESS, "localhost");
			// necessary for loading the web-submission extension
			config.setString(JobManagerOptions.ADDRESS, "localhost");
			try {
				restConfig = RestServerEndpointConfiguration.fromConfiguration(config);
			} catch (ConfigurationException e) {
				throw new RuntimeException("Implementation error. RestServerEndpointConfiguration#fromConfiguration failed for default configuration.");
			}
			handlerConfig = RestHandlerConfiguration.fromConfiguration(config);
			executor = Executors.directExecutor();

			dispatcherGatewayRetriever = () -> null;
			resourceManagerGatewayRetriever = () -> null;
			metricQueryServiceRetriever = path -> null;
		}

		private DocumentingDispatcherRestEndpoint() throws IOException {
			super(
				restConfig,
				dispatcherGatewayRetriever,
				config,
				handlerConfig,
				resourceManagerGatewayRetriever,
				NoOpTransientBlobService.INSTANCE,
				executor,
				metricQueryServiceRetriever,
				NoOpElectionService.INSTANCE,
				NoOpFatalErrorHandler.INSTANCE);
		}

		@Override
		public List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
			return super.initializeHandlers(restAddressFuture);
		}

		private enum NoOpElectionService implements LeaderElectionService {
			INSTANCE;
			@Override
			public void start(final LeaderContender contender) throws Exception {

			}

			@Override
			public void stop() throws Exception {

			}

			@Override
			public void confirmLeaderSessionID(final UUID leaderSessionID) {

			}

			@Override
			public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
				return false;
			}
		}

		private enum NoOpFatalErrorHandler implements FatalErrorHandler {
			INSTANCE;

			@Override
			public void onFatalError(final Throwable exception) {

			}
		}
	}

	/**
	 * Interface to expose the supported {@link MessageHeaders} of a {@link RestServerEndpoint}.
	 */
	private interface DocumentingRestEndpoint {
		List<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture);

		default List<MessageHeaders> getSpecs() {
			Comparator<String> comparator = new RestServerEndpoint.RestHandlerUrlComparator.CaseInsensitiveOrderComparator();
			return initializeHandlers(CompletableFuture.completedFuture(null)).stream()
				.map(tuple -> tuple.f0)
				.filter(spec -> spec instanceof MessageHeaders)
				.map(spec -> (MessageHeaders) spec)
				.sorted((spec1, spec2) -> comparator.compare(spec1.getTargetRestEndpointURL(), spec2.getTargetRestEndpointURL()))
				.collect(Collectors.toList());
		}
	}
}
