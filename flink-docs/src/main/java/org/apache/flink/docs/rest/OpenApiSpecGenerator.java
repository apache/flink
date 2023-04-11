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
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.FileUploadHandler;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.json.SerializedThrowableSerializer;
import org.apache.flink.runtime.rest.util.DocumentingRestEndpoint;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.webmonitor.handlers.JarUploadHeaders;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import io.swagger.v3.core.converter.AnnotatedType;
import io.swagger.v3.core.converter.ModelConverterContext;
import io.swagger.v3.core.converter.ModelConverterContextImpl;
import io.swagger.v3.core.jackson.ModelResolver;
import io.swagger.v3.core.jackson.TypeNameResolver;
import io.swagger.v3.core.util.Yaml;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.ComposedSchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OpenAPI spec generator for the Rest API.
 *
 * <p>One OpenAPI yml file is generated for each {@link RestServerEndpoint} implementation that can
 * be embedded into .md files using {@code {% include ${generated.docs.dir}/file.yml %}}.
 */
public class OpenApiSpecGenerator {

    private static final ModelConverterContext modelConverterContext;

    static {
        ModelResolver.enumsAsRef = true;
        final ObjectMapper mapper =
                JacksonMapperFactory.createObjectMapper()
                        .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        modelConverterContext =
                new ModelConverterContextImpl(
                        Collections.singletonList(
                                new ModelResolver(
                                        mapper, new NameClashDetectingTypeNameResolver())));
    }

    @VisibleForTesting
    static void createDocumentationFile(
            String title,
            DocumentingRestEndpoint restEndpoint,
            RestAPIVersion apiVersion,
            Path outputFile)
            throws IOException {
        final OpenAPI openApi = createDocumentation(title, restEndpoint, apiVersion);
        Files.deleteIfExists(outputFile);
        Files.write(outputFile, Yaml.pretty(openApi).getBytes(StandardCharsets.UTF_8));
    }

    @VisibleForTesting
    static OpenAPI createDocumentation(
            String title, DocumentingRestEndpoint restEndpoint, RestAPIVersion apiVersion) {
        final OpenAPI openApi = new OpenAPI();

        // eagerly initialize some data-structures to simplify operations later on
        openApi.setPaths(new io.swagger.v3.oas.models.Paths());
        openApi.setComponents(new Components());

        setInfo(openApi, title, apiVersion);

        List<MessageHeaders> specs =
                restEndpoint.getSpecs().stream()
                        .filter(spec -> spec.getSupportedAPIVersions().contains(apiVersion))
                        .filter(ApiSpecGeneratorUtils::shouldBeDocumented)
                        .collect(Collectors.toList());
        final Set<String> usedOperationIds = new HashSet<>();
        specs.forEach(spec -> add(spec, openApi, usedOperationIds));

        final List<Schema> asyncOperationSchemas = collectAsyncOperationResultVariants(specs);

        // this adds the schema for every JSON object
        openApi.components(
                new Components().schemas(new HashMap<>(modelConverterContext.getDefinedModels())));

        injectAsyncOperationResultSchema(openApi, asyncOperationSchemas);

        overrideIdSchemas(openApi);
        overrideSerializeThrowableSchema(openApi);

        sortProperties(openApi);
        sortSchemas(openApi);

        return openApi;
    }

    @SuppressWarnings("rawtypes")
    private static void sortProperties(OpenAPI openApi) {
        for (Schema<?> schema : openApi.getComponents().getSchemas().values()) {
            final Map<String, Schema> properties = schema.getProperties();
            if (properties != null) {
                final LinkedHashMap<String, Schema> sortedMap = new LinkedHashMap<>();
                properties.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
                schema.setProperties(sortedMap);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private static void sortSchemas(OpenAPI openApi) {
        Components components = openApi.getComponents();
        Map<String, Schema> schemas = components.getSchemas();
        final LinkedHashMap<String, Schema> sortedSchemas = new LinkedHashMap<>();
        schemas.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> sortedSchemas.put(entry.getKey(), entry.getValue()));
        components.setSchemas(sortedSchemas);
    }

    private static void setInfo(
            final OpenAPI openApi, String title, final RestAPIVersion apiVersion) {
        openApi.info(
                new Info()
                        .title(title)
                        .version(
                                String.format(
                                        "%s/%s",
                                        apiVersion.getURLVersionPrefix(),
                                        EnvironmentInformation.getVersion()))
                        .contact(new Contact().email("user@flink.apache.org"))
                        .license(
                                new License()
                                        .name("Apache 2.0")
                                        .url("https://www.apache.org/licenses/LICENSE-2.0.html")));
    }

    private static List<Schema> collectAsyncOperationResultVariants(
            final Collection<MessageHeaders> specs) {
        return specs.stream()
                .filter(spec -> spec instanceof AsynchronousOperationStatusMessageHeaders)
                .map(
                        spec ->
                                ((AsynchronousOperationStatusMessageHeaders<?, ?>) spec)
                                        .getValueClass())
                .distinct()
                .sorted(Comparator.comparing(Class::getSimpleName))
                .map(clazz -> getSchema(clazz))
                .collect(Collectors.toList());
    }

    /**
     * The {@link AsynchronousOperationResult} contains a generic 'operation' field that can't be
     * properly extracted from swagger. This method injects these manually.
     *
     * <p>Resulting spec diff:
     *
     * <pre>
     * AsynchronousOperationResult:
     *   type: object
     *   properties:
     *     operation:
     * -     type: object
     * +       oneOf:
     * +       - $ref: '#/components/schemas/AsynchronousOperationInfo'
     * +       - $ref: '#/components/schemas/SavepointInfo'
     * </pre>
     */
    private static void injectAsyncOperationResultSchema(
            final OpenAPI openApi, List<Schema> asyncOperationSchemas) {
        final Schema schema =
                openApi.getComponents()
                        .getSchemas()
                        .get(AsynchronousOperationResult.class.getSimpleName());
        if (schema != null) {
            schema.getProperties()
                    .put(
                            AsynchronousOperationResult.FIELD_NAME_OPERATION,
                            new ComposedSchema().oneOf(asyncOperationSchemas));
        }
    }

    /**
     * Various ID classes are effectively internal classes that aren't sufficiently annotated to
     * work with automatic schema extraction. This method overrides the schema of these to a string
     * regex pattern.
     *
     * <p>Resulting spec diff:
     *
     * <pre>
     * JobID:
     * - type: object
     * - properties:
     * -  upperPart:
     * -     type: integer
     * -     format: int64
     * -   lowerPart:
     * -     type: integer
     * -     format: int64
     * -   bytes:
     * -     type: array
     * -     items:
     * -       type: string
     * -       format: byte
     * + pattern: "[0-9a-f]{32}"
     * + type: string
     * </pre>
     */
    private static void overrideIdSchemas(final OpenAPI openApi) {
        final Schema idSchema = new Schema().type("string").pattern("[0-9a-f]{32}");

        openApi.getComponents()
                .addSchemas(JobID.class.getSimpleName(), idSchema)
                .addSchemas(JobVertexID.class.getSimpleName(), idSchema)
                .addSchemas(IntermediateDataSetID.class.getSimpleName(), idSchema)
                .addSchemas(TriggerId.class.getSimpleName(), idSchema)
                .addSchemas(ResourceID.class.getSimpleName(), idSchema);
    }

    private static void overrideSerializeThrowableSchema(final OpenAPI openAPI) {
        final Schema serializedThrowableSchema =
                new Schema<>()
                        .type("object")
                        .properties(
                                Collections.singletonMap(
                                        SerializedThrowableSerializer
                                                .FIELD_NAME_SERIALIZED_THROWABLE,
                                        new Schema().type("string").format("binary")));

        openAPI.getComponents()
                .addSchemas(SerializedThrowable.class.getSimpleName(), serializedThrowableSchema);
    }

    private static void add(
            MessageHeaders<?, ?, ?> spec, OpenAPI openApi, Set<String> usedOperationIds) {
        final PathItem pathItem =
                openApi.getPaths()
                        .computeIfAbsent(
                                // convert netty to openapi syntax
                                // ":parameter" -> "{parameter}"
                                spec.getTargetRestEndpointURL().replaceAll(":([\\w]+)", "{$1}"),
                                ignored -> new PathItem());

        final Operation operation = new Operation();

        operation.description(spec.getDescription());

        setOperationId(operation, spec, usedOperationIds);
        setParameters(operation, spec);
        setRequest(operation, spec);
        setResponse(operation, spec);

        pathItem.operation(convert(spec.getHttpMethod()), operation);
    }

    private static void setOperationId(
            final Operation operation,
            final MessageHeaders<?, ?, ?> spec,
            Set<String> usedOperationIds) {
        final String operationId = spec.operationId();

        if (!usedOperationIds.add(operationId)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate OperationId '%s' for path '%s'",
                            operationId, spec.getTargetRestEndpointURL()));
        }
        operation.setOperationId(operationId);
    }

    private static void setParameters(
            final Operation operation, final MessageHeaders<?, ?, ?> spec) {
        List<Parameter> parameters = new ArrayList<>();
        for (MessagePathParameter<?> pathParameter :
                spec.getUnresolvedMessageParameters().getPathParameters()) {
            parameters.add(
                    new Parameter()
                            .name(pathParameter.getKey())
                            .in("path")
                            .required(pathParameter.isMandatory())
                            .description(pathParameter.getDescription())
                            .schema(getSchema(getParameterType(pathParameter))));
        }
        for (MessageQueryParameter<?> queryParameter :
                spec.getUnresolvedMessageParameters().getQueryParameters()) {
            parameters.add(
                    new Parameter()
                            .name(queryParameter.getKey())
                            .in("query")
                            .required(queryParameter.isMandatory())
                            .description(queryParameter.getDescription())
                            .schema(getSchema(getParameterType(queryParameter)))
                            .style(Parameter.StyleEnum.FORM));
        }
        if (!parameters.isEmpty()) {
            operation.parameters(parameters);
        }
    }

    private static <T extends MessagePathParameter<?>> Type getParameterType(T o) {
        Class<?> clazz = o.getClass();
        while (clazz.getSuperclass() != MessagePathParameter.class) {
            clazz = clazz.getSuperclass();
        }
        return ((ParameterizedType) o.getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
    }

    private static <T extends MessageQueryParameter<?>> Type getParameterType(T o) {
        Class<?> clazz = o.getClass();
        while (clazz.getSuperclass() != MessageQueryParameter.class) {
            clazz = clazz.getSuperclass();
        }
        return ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments()[0];
    }

    private static void setRequest(final Operation operation, final MessageHeaders<?, ?, ?> spec) {
        // empty request bodies should not be documented at all
        if (spec.getRequestClass() != EmptyRequestBody.class) {
            operation.requestBody(
                    new RequestBody()
                            .content(
                                    createContentWithMediaType(
                                            "application/json",
                                            getSchema(spec.getRequestClass()))));
        }

        // files upload response schemas cannot be generated automatically; do it manually
        if (spec.acceptsFileUploads()) {
            injectFileUploadRequest(spec, operation);
        }
    }

    private static void injectFileUploadRequest(
            final MessageHeaders<?, ?, ?> spec, final Operation operation) {
        // TODO: unhack
        if (spec instanceof JarUploadHeaders) {
            operation.requestBody(
                    new RequestBody()
                            .required(true)
                            .content(
                                    new Content()
                                            .addMediaType(
                                                    "application/x-java-archive",
                                                    new MediaType()
                                                            .schema(
                                                                    new Schema<>()
                                                                            .type("string")
                                                                            .format("binary")))));
        }

        // TODO: unhack
        if (spec instanceof JobSubmitHeaders) {
            operation.requestBody(
                    new RequestBody()
                            .required(true)
                            .content(
                                    createContentWithMediaType(
                                            "multipart/form-data",
                                            new Schema<>()
                                                    .addProperties(
                                                            FileUploadHandler
                                                                    .HTTP_ATTRIBUTE_REQUEST,
                                                            getSchema(spec.getRequestClass()))
                                                    .addProperties(
                                                            "filename",
                                                            new ArraySchema()
                                                                    .items(
                                                                            new Schema<>()
                                                                                    .type("string")
                                                                                    .format(
                                                                                            "binary"))))));
        }
    }

    private static void setResponse(final Operation operation, final MessageHeaders<?, ?, ?> spec) {
        final ApiResponse apiResponse = new ApiResponse();
        if (spec.getResponseClass() != EmptyResponseBody.class) {
            apiResponse.content(
                    createContentWithMediaType(
                            "application/json", getSchema(spec.getResponseClass())));
        }
        operation.responses(
                new ApiResponses()
                        .addApiResponse(
                                Integer.toString(spec.getResponseStatusCode().code()),
                                apiResponse.description("The request was successful.")));
    }

    private static Content createContentWithMediaType(String mediaType, Schema schema) {
        return new Content().addMediaType(mediaType, new MediaType().schema(schema));
    }

    private static Schema<?> getSchema(Type type) {
        final AnnotatedType annotatedType = new AnnotatedType(type).resolveAsRef(true);
        final Schema<?> schema = modelConverterContext.resolve(annotatedType);
        if (type instanceof Class<?>) {
            final Class<?> clazz = (Class<?>) type;
            ApiSpecGeneratorUtils.findAdditionalFieldType(clazz)
                    .map(OpenApiSpecGenerator::getSchema)
                    .ifPresent(
                            additionalPropertiesSchema -> {
                                // We need to update the schema of the component, that is referenced
                                // by the resolved schema (because we're setting resolveAsRef to
                                // true).
                                final String referencedComponentName = clazz.getSimpleName();
                                final Schema<?> referencedComponentSchema =
                                        Preconditions.checkNotNull(
                                                modelConverterContext
                                                        .getDefinedModels()
                                                        .get(referencedComponentName),
                                                "Schema of the referenced component [%s] was not found.",
                                                referencedComponentName);
                                referencedComponentSchema.setAdditionalProperties(
                                        additionalPropertiesSchema);
                            });
        }
        return schema;
    }

    private static PathItem.HttpMethod convert(HttpMethodWrapper wrapper) {
        switch (wrapper) {
            case GET:
                return PathItem.HttpMethod.GET;
            case POST:
                return PathItem.HttpMethod.POST;
            case DELETE:
                return PathItem.HttpMethod.DELETE;
            case PATCH:
                return PathItem.HttpMethod.PATCH;
            case PUT:
                return PathItem.HttpMethod.PUT;
        }
        throw new IllegalArgumentException("not supported");
    }

    /** A {@link TypeNameResolver} that detects name-clashes between top-level and inner classes. */
    public static class NameClashDetectingTypeNameResolver extends TypeNameResolver {
        private final Map<String, String> seenClassNamesToFQCN = new HashMap<>();

        @Override
        protected String getNameOfClass(Class<?> cls) {
            final String modelName = super.getNameOfClass(cls);

            final String fqcn = cls.getCanonicalName();
            final String previousFqcn = seenClassNamesToFQCN.put(modelName, fqcn);

            if (previousFqcn != null && !fqcn.equals(previousFqcn)) {
                throw new IllegalStateException(
                        String.format(
                                "Detected name clash for model name '%s'.%n"
                                        + "\tClasses:%n"
                                        + "\t\t- %s%n"
                                        + "\t\t- %s%n"
                                        + "\tEither rename the classes or annotate them with '%s' and set a unique 'name'.",
                                modelName, fqcn, previousFqcn, Schema.class.getCanonicalName()));
            }

            return modelName;
        }
    }
}
