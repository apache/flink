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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.util.DocumentingDispatcherRestEndpoint;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.util.ConfigurationException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.SerializableString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.CharacterEscapes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.io.SerializedString;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.docs.util.Utils.escapeCharacters;

/**
 * Generator for the Rest API documentation.
 *
 * <p>One HTML file is generated for each {@link RestServerEndpoint} implementation that can be
 * embedded into .md files using {@code {% include ${generated.docs.dir}/file.html %}}. Each file
 * contains a series of HTML tables, one for each REST call.
 *
 * <p>The generated table for each REST call looks like this:
 * ---------------------------------------------------------- | URL |
 * ---------------------------------------------------------- | Verb: verb (GET|POST|...) | Response
 * code: responseCode| ---------------------------------------------------------- | Path parameters
 * (if any are defined) | ---------------------------------------------------------- | -
 * parameterName: description | | ... | ---------------------------------------------------------- |
 * Query parameters (if any are defined) |
 * ---------------------------------------------------------- | - parameterName (requisiteness):
 * description | | ... | ---------------------------------------------------------- | Request json
 * schema (a collapsible "Request" button) |
 * ---------------------------------------------------------- | Response json schema (a collapsible
 * "Response" button) | ----------------------------------------------------------
 */
public class RestAPIDocGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(RestAPIDocGenerator.class);

    private static final ObjectMapper mapper;
    private static final JsonSchemaGenerator schemaGen;

    static {
        mapper = new ObjectMapper();
        mapper.getFactory().setCharacterEscapes(new HTMLCharacterEscapes());
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        schemaGen = new JsonSchemaGenerator(mapper);
    }

    /**
     * Generates the REST API documentation.
     *
     * @param args args[0] contains the directory into which the generated files are placed
     * @throws IOException if any file operation failed
     */
    public static void main(String[] args) throws IOException, ConfigurationException {
        String outputDirectory = args[0];

        for (final RestAPIVersion apiVersion : RestAPIVersion.values()) {
            if (apiVersion == RestAPIVersion.V0) {
                // this version exists only for testing purposes
                continue;
            }
            createHtmlFile(
                    new DocumentingDispatcherRestEndpoint(),
                    apiVersion,
                    Paths.get(
                            outputDirectory,
                            "rest_" + apiVersion.getURLVersionPrefix() + "_dispatcher.html"));
        }
    }

    @VisibleForTesting
    static void createHtmlFile(
            DocumentingRestEndpoint restEndpoint, RestAPIVersion apiVersion, Path outputFile)
            throws IOException {
        StringBuilder html = new StringBuilder();

        List<MessageHeaders> specs =
                restEndpoint.getSpecs().stream()
                        .filter(spec -> spec.getSupportedAPIVersions().contains(apiVersion))
                        .filter(RestAPIDocGenerator::shouldBeDocumented)
                        .collect(Collectors.toList());
        specs.forEach(spec -> html.append(createHtmlEntry(spec)));

        Files.deleteIfExists(outputFile);
        Files.write(outputFile, html.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static boolean shouldBeDocumented(MessageHeaders spec) {
        return spec.getClass().getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    private static String createHtmlEntry(MessageHeaders<?, ?, ?> spec) {
        Class<?> nestedAsyncOperationResultClass = null;
        if (spec instanceof AsynchronousOperationStatusMessageHeaders) {
            nestedAsyncOperationResultClass =
                    ((AsynchronousOperationStatusMessageHeaders<?, ?>) spec).getValueClass();
        }
        String requestEntry =
                createMessageHtmlEntry(spec.getRequestClass(), null, EmptyRequestBody.class);
        String responseEntry =
                createMessageHtmlEntry(
                        spec.getResponseClass(),
                        nestedAsyncOperationResultClass,
                        EmptyResponseBody.class);

        String pathParameterList =
                createPathParameterHtmlList(
                        spec.getUnresolvedMessageParameters().getPathParameters());
        String queryParameterList =
                createQueryParameterHtmlList(
                        spec.getUnresolvedMessageParameters().getQueryParameters());

        StringBuilder sb = new StringBuilder();
        {
            sb.append("<table class=\"rest-api table table-bordered\">\n");
            sb.append("  <tbody>\n");
            sb.append("    <tr>\n");
            sb.append(
                    "      <td class=\"text-left\" colspan=\"2\"><h5><strong>"
                            + spec.getTargetRestEndpointURL()
                            + "</strong></h5></td>\n");
            sb.append("    </tr>\n");
            sb.append("    <tr>\n");
            sb.append(
                    "      <td class=\"text-left\" style=\"width: 20%\">Verb: <code>"
                            + spec.getHttpMethod()
                            + "</code></td>\n");
            sb.append(
                    "      <td class=\"text-left\">Response code: <code>"
                            + spec.getResponseStatusCode()
                            + "</code></td>\n");
            sb.append("    </tr>\n");
            sb.append("    <tr>\n");
            sb.append(
                    "      <td colspan=\"2\">"
                            + escapeCharacters(spec.getDescription())
                            + "</td>\n");
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
        int reqHash =
                spec.getTargetRestEndpointURL().hashCode()
                        + spec.getHttpMethod().name().hashCode()
                        + spec.getRequestClass().getCanonicalName().hashCode();
        int resHash =
                spec.getTargetRestEndpointURL().hashCode()
                        + spec.getHttpMethod().name().hashCode()
                        + spec.getResponseClass().getCanonicalName().hashCode();
        {
            sb.append("    <tr>\n");
            sb.append("      <td colspan=\"2\">\n");
            sb.append("      <div class=\"book-expand\">\n");
            sb.append("        <label>\n");
            sb.append("          <div class=\"book-expand-head flex justify-between\">\n");
            sb.append("            <span>Request</span>\n");
            sb.append("            &nbsp;");
            sb.append("            <span>▾</span>\n");
            sb.append("          </div>\n");
            sb.append("          <input type=\"checkbox\" class=\"hidden\">\n");
            sb.append("          <div class=\"book-expand-content markdown-inner\">\n");
            sb.append("          <pre>\n");
            sb.append("            <code>\n");
            sb.append(requestEntry);
            sb.append("            </code>\n");
            sb.append("          </pre>\n");
            sb.append("          </div>\n");
            sb.append("        </label>\n");
            sb.append("      </div>\n");
            sb.append("      </td>\n");
            sb.append("    </tr>\n");
            sb.append("    <tr>\n");
            sb.append("      <td colspan=\"2\">\n");
            sb.append("      <div class=\"book-expand\">\n");
            sb.append("        <label>\n");
            sb.append("          <div class=\"book-expand-head flex justify-between\">\n");
            sb.append("            <span>Response</span>\n");
            sb.append("            &nbsp;");
            sb.append("            <span>▾</span>\n");
            sb.append("          </div>\n");
            sb.append("          <input type=\"checkbox\" class=\"hidden\">\n");
            sb.append("          <div class=\"book-expand-content markdown-inner\">\n");
            sb.append("          <pre>\n");
            sb.append("            <code>\n");
            sb.append(responseEntry);
            sb.append("            </code>\n");
            sb.append("          </pre>\n");
            sb.append("          </div>\n");
            sb.append("        </label>\n");
            sb.append("      </div>\n");
            sb.append("      </td>\n");
            sb.append("    </tr>\n");
            sb.append("  </tbody>\n");
            sb.append("</table>\n");
        }

        return sb.toString();
    }

    private static String createPathParameterHtmlList(
            Collection<MessagePathParameter<?>> pathParameters) {
        StringBuilder pathParameterList = new StringBuilder();
        pathParameters.forEach(
                messagePathParameter ->
                        pathParameterList.append(
                                String.format(
                                        "<li><code>%s</code> - %s</li>\n",
                                        messagePathParameter.getKey(),
                                        messagePathParameter.getDescription())));
        return pathParameterList.toString();
    }

    private static String createQueryParameterHtmlList(
            Collection<MessageQueryParameter<?>> queryParameters) {
        StringBuilder queryParameterList = new StringBuilder();
        queryParameters.stream()
                .sorted(
                        (param1, param2) ->
                                Boolean.compare(param1.isMandatory(), param2.isMandatory()))
                .forEach(
                        parameter ->
                                queryParameterList.append(
                                        String.format(
                                                "<li><code>%s</code> (%s): %s</li>\n",
                                                parameter.getKey(),
                                                parameter.isMandatory() ? "mandatory" : "optional",
                                                parameter.getDescription())));
        return queryParameterList.toString();
    }

    private static String createMessageHtmlEntry(
            Class<?> messageClass,
            @Nullable Class<?> nestedAsyncOperationResultClass,
            Class<?> emptyMessageClass) {
        JsonSchema schema = generateSchema(messageClass);

        if (nestedAsyncOperationResultClass != null) {
            JsonSchema innerSchema = generateSchema(nestedAsyncOperationResultClass);
            schema.asObjectSchema()
                    .getProperties()
                    .put(AsynchronousOperationResult.FIELD_NAME_OPERATION, innerSchema);
        }

        String json;
        if (messageClass == emptyMessageClass) {
            json = "{}";
        } else {
            try {
                json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
            } catch (JsonProcessingException e) {
                LOG.error(
                        "Failed to write message schema for class {}.",
                        messageClass.getCanonicalName(),
                        e);
                throw new RuntimeException(
                        "Failed to write message schema for class "
                                + messageClass.getCanonicalName()
                                + ".",
                        e);
            }
        }

        return json;
    }

    private static JsonSchema generateSchema(Class<?> messageClass) {
        try {
            return schemaGen.generateSchema(messageClass);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to generate message schema for class {}.", messageClass, e);
            throw new RuntimeException(
                    "Failed to generate message schema for class "
                            + messageClass.getCanonicalName()
                            + ".",
                    e);
        }
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
}
