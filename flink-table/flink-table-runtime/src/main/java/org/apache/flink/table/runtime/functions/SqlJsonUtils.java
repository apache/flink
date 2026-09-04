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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.api.JsonQueryOnEmptyOrError;
import org.apache.flink.table.api.JsonQueryWrapper;
import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.flink.shaded.com.jayway.jsonpath.Configuration;
import org.apache.flink.shaded.com.jayway.jsonpath.DocumentContext;
import org.apache.flink.shaded.com.jayway.jsonpath.InvalidPathException;
import org.apache.flink.shaded.com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.com.jayway.jsonpath.Option;
import org.apache.flink.shaded.com.jayway.jsonpath.PathNotFoundException;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.cache.CacheProvider;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import org.apache.flink.shaded.com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for JSON functions.
 *
 * <p>Note that these methods are called from generated code.
 */
@Internal
public class SqlJsonUtils {

    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    private static final ObjectMapper MAPPER =
            new ObjectMapper(JSON_FACTORY)
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
    private static final Pattern JSON_PATH_BASE =
            Pattern.compile(
                    "^\\s*(?<mode>strict|lax)\\s+(?<spec>.+)$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private static final JacksonJsonProvider JSON_PATH_JSON_PROVIDER =
            new JacksonJsonProvider(MAPPER);
    private static final MappingProvider JSON_PATH_MAPPING_PROVIDER =
            new JacksonMappingProvider(MAPPER);

    /**
     * Configuration for JSON_LENGTH, which evaluates plain paths only and therefore does not need
     * the 'lax'/'strict' path mode handling of {@link #jsonApiCommonSyntax}. Exceptions are left
     * unsuppressed so that a path resolving to a JSON null literal (returns {@code null}) stays
     * distinguishable from a path that does not exist (throws {@link PathNotFoundException}).
     * {@link Configuration} is immutable, so a single instance is shared across all calls.
     */
    private static final Configuration JSON_PATH_LENGTH_CONFIG =
            Configuration.builder()
                    .jsonProvider(JSON_PATH_JSON_PROVIDER)
                    .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                    .build();

    private static final String JSON_QUERY_FUNCTION_NAME = "JSON_QUERY";
    private static final String JSON_VALUE_FUNCTION_NAME = "JSON_VALUE";
    private static final String JSON_EXISTS_FUNCTION_NAME = "JSON_EXISTS";

    private static final Configuration JSON_PATH_TYPE_CONFIG =
            Configuration.builder()
                    .jsonProvider(JSON_PATH_JSON_PROVIDER)
                    .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                    .build();

    private SqlJsonUtils() {}

    static {
        CacheProvider.setCache(new JsonPathCache());
    }

    /** Returns the {@link JsonNodeFactory} for creating nodes. */
    public static JsonNodeFactory getNodeFactory() {
        return MAPPER.getNodeFactory();
    }

    /** Returns a new {@link ObjectNode}. */
    public static ObjectNode createObjectNode() {
        return MAPPER.createObjectNode();
    }

    /** Returns a new {@link ArrayNode}. */
    public static ArrayNode createArrayNode() {
        return MAPPER.createArrayNode();
    }

    /** Serializes the given {@link JsonNode} to a JSON string. */
    public static String serializeJson(JsonNode node) {
        try {
            // For JSON functions to have deterministic output, we need to sort the keys. However,
            // Jackson's built-in features don't work on the tree representation, so we need to
            // convert the tree first.
            final Object convertedNode = MAPPER.treeToValue(node, Object.class);
            return MAPPER.writeValueAsString(convertedNode);
        } catch (JsonProcessingException e) {
            throw new TableRuntimeException(
                    "JSON object could not be serialized: " + node.asText(), e);
        }
    }

    public static Boolean jsonExists(String input, String pathSpec) {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec), JsonExistsOnError.FALSE);
    }

    public static Boolean jsonExists(
            String input, String pathSpec, JsonExistsOnError errorBehavior) {
        return jsonExists(jsonApiCommonSyntax(input, pathSpec), errorBehavior);
    }

    private static Boolean jsonExists(JsonPathContext context, JsonExistsOnError errorBehavior) {
        if (context.hasException()) {
            switch (errorBehavior) {
                case TRUE:
                    return Boolean.TRUE;
                case FALSE:
                    return Boolean.FALSE;
                case ERROR:
                    throw toUnchecked(context.exc);
                case UNKNOWN:
                    return null;
                default:
                    throw illegalErrorBehaviorFunc(
                            errorBehavior.toString(), JSON_EXISTS_FUNCTION_NAME);
            }
        } else {
            return context.obj != null;
        }
    }

    public static Object jsonValue(
            String input,
            String pathSpec,
            JsonValueOnEmptyOrError emptyBehavior,
            Object defaultValueOnEmpty,
            JsonValueOnEmptyOrError errorBehavior,
            Object defaultValueOnError) {
        return jsonValue(
                jsonApiCommonSyntax(input, pathSpec),
                emptyBehavior,
                defaultValueOnEmpty,
                errorBehavior,
                defaultValueOnError);
    }

    /** Accepts a pre-parsed context from {@link #jsonParse}. */
    public static Object jsonValue(
            JsonValueContext parsedInput,
            String pathSpec,
            JsonValueOnEmptyOrError emptyBehavior,
            Object defaultValueOnEmpty,
            JsonValueOnEmptyOrError errorBehavior,
            Object defaultValueOnError) {
        return jsonValue(
                jsonApiCommonSyntax(parsedInput, pathSpec),
                emptyBehavior,
                defaultValueOnEmpty,
                errorBehavior,
                defaultValueOnError);
    }

    private static Object jsonValue(
            JsonPathContext context,
            JsonValueOnEmptyOrError emptyBehavior,
            Object defaultValueOnEmpty,
            JsonValueOnEmptyOrError errorBehavior,
            Object defaultValueOnError) {
        final Exception exc;
        if (context.hasException()) {
            exc = context.exc;
        } else {
            Object value = context.obj;
            if (value == null || context.mode == PathMode.LAX && !isScalarObject(value)) {
                switch (emptyBehavior) {
                    case ERROR:
                        throw emptyResultOfJsonValueFuncNotAllowed();
                    case NULL:
                        return null;
                    case DEFAULT:
                        return defaultValueOnEmpty;
                    default:
                        throw illegalEmptyBehaviorFunc(
                                emptyBehavior.toString(), JSON_VALUE_FUNCTION_NAME);
                }
            } else if (context.mode == PathMode.STRICT && !isScalarObject(value)) {
                exc = scalarValueRequiredInStrictModeOfJsonValueFunc(value.toString());
            } else {
                return value;
            }
        }
        switch (errorBehavior) {
            case ERROR:
                throw toUnchecked(exc);
            case NULL:
                return null;
            case DEFAULT:
                return defaultValueOnError;
            default:
                throw illegalErrorBehaviorFunc(errorBehavior.toString(), JSON_VALUE_FUNCTION_NAME);
        }
    }

    public enum JsonQueryReturnType {
        STRING,
        ARRAY,
        RAW_ARRAY
    }

    public static Object jsonQuery(
            String input,
            String pathSpec,
            JsonQueryReturnType returnType,
            JsonQueryWrapper wrapperBehavior,
            JsonQueryOnEmptyOrError emptyBehavior,
            JsonQueryOnEmptyOrError errorBehavior) {
        return jsonQuery(
                jsonApiCommonSyntax(input, pathSpec),
                returnType,
                wrapperBehavior,
                emptyBehavior,
                errorBehavior);
    }

    /** Like {@link #jsonQuery} but accepts a pre-parsed context from {@link #jsonParse}. */
    public static Object jsonQueryParsed(
            JsonValueContext parsedInput,
            String pathSpec,
            JsonQueryReturnType returnType,
            JsonQueryWrapper wrapperBehavior,
            JsonQueryOnEmptyOrError emptyBehavior,
            JsonQueryOnEmptyOrError errorBehavior) {
        return jsonQuery(
                jsonApiCommonSyntax(parsedInput, pathSpec),
                returnType,
                wrapperBehavior,
                emptyBehavior,
                errorBehavior);
    }

    private static Object jsonQuery(
            JsonPathContext context,
            JsonQueryReturnType returnType,
            JsonQueryWrapper wrapperBehavior,
            JsonQueryOnEmptyOrError emptyBehavior,
            JsonQueryOnEmptyOrError errorBehavior) {
        final Exception exc;
        if (context.hasException()) {
            exc = context.exc;
        } else {
            Object value;
            if (context.obj == null) {
                value = null;
            } else {
                switch (wrapperBehavior) {
                    case WITHOUT_ARRAY:
                        value = context.obj;
                        break;
                    case UNCONDITIONAL_ARRAY:
                        value = Collections.singletonList(context.obj);
                        break;
                    case CONDITIONAL_ARRAY:
                        if (context.obj instanceof Collection) {
                            value = context.obj;
                        } else {
                            value = Collections.singletonList(context.obj);
                        }
                        break;
                    default:
                        throw illegalWrapperBehaviorInJsonQueryFunc(wrapperBehavior.toString());
                }
            }
            if (value == null || context.mode == PathMode.LAX && isScalarObject(value)) {
                return emptyResultForJsonQuery(emptyBehavior, returnType);
            } else if (context.mode == PathMode.STRICT && isScalarObject(value)) {
                exc = arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(value.toString());
            } else {
                try {
                    switch (returnType) {
                        case STRING:
                            return jsonize(value);
                        case ARRAY:
                            final List<Object> list = (List<Object>) value;
                            final Object[] arr = new Object[list.size()];
                            for (int i = 0; i < list.size(); i++) {
                                final Object el = list.get(i);
                                if (el != null) {
                                    final String stringifiedEl;
                                    if (isScalarObject(el)) {
                                        stringifiedEl = String.valueOf(el);
                                    } else {
                                        stringifiedEl = jsonize(el);
                                    }
                                    arr[i] = StringData.fromString(stringifiedEl);
                                }
                            }

                            return new GenericArrayData(arr);
                        case RAW_ARRAY:
                            final List<Object> rawList = (List<Object>) value;
                            return rawList.toArray();
                        default:
                            throw new TableRuntimeException("illegal return type");
                    }
                } catch (Exception e) {
                    exc = e;
                }
            }
        }
        return errorResultForJsonQuery(errorBehavior, returnType, exc);
    }

    private static Object emptyResultForJsonQuery(
            JsonQueryOnEmptyOrError emptyBehavior, JsonQueryReturnType returnType) {
        switch (emptyBehavior) {
            case ERROR:
                throw emptyResultOfJsonQueryFuncNotAllowed();
            case NULL:
                return null;
            case EMPTY_ARRAY:
                switch (returnType) {
                    case ARRAY:
                        return new GenericArrayData(new StringData[0]);
                    case RAW_ARRAY:
                        return new Object[0];
                    case STRING:
                        return "[]";
                    default:
                        throw new RuntimeException("illegal return type");
                }
            case EMPTY_OBJECT:
                if (Objects.requireNonNull(returnType) == JsonQueryReturnType.STRING) {
                    return "{}";
                }
                throw illegalEmptyBehaviorFunc(emptyBehavior.toString(), JSON_QUERY_FUNCTION_NAME);
            default:
                throw illegalEmptyBehaviorFunc(emptyBehavior.toString(), JSON_QUERY_FUNCTION_NAME);
        }
    }

    private static Object errorResultForJsonQuery(
            JsonQueryOnEmptyOrError errorBehaviour, JsonQueryReturnType returnType, Exception exc) {
        switch (errorBehaviour) {
            case ERROR:
                throw toUnchecked(exc);
            case NULL:
                return null;
            case EMPTY_ARRAY:
                switch (returnType) {
                    case ARRAY:
                        return new GenericArrayData(new StringData[0]);
                    case RAW_ARRAY:
                        return new Object[0];
                    case STRING:
                        return "[]";
                    default:
                        throw new TableRuntimeException("illegal return type");
                }
            case EMPTY_OBJECT:
                if (Objects.requireNonNull(returnType) == JsonQueryReturnType.STRING) {
                    return "{}";
                }
                throw illegalErrorBehaviorFunc(errorBehaviour.toString(), JSON_QUERY_FUNCTION_NAME);
            default:
                throw illegalErrorBehaviorFunc(errorBehaviour.toString(), JSON_QUERY_FUNCTION_NAME);
        }
    }

    /** Accepts a pre-parsed context from {@link #jsonParse}. */
    public static Integer jsonLength(final JsonValueContext parsedInput) {
        if (parsedInput == null || parsedInput.hasException()) {
            return null;
        }

        // Whole document: a top-level JSON null literal counts as a scalar (length 1).
        return jsonLengthValue(parsedInput.obj);
    }

    /**
     * Accepts a pre-parsed context from {@link #jsonParse}. {@code isPathDefinite} is computed at
     * plan time by {@link #isPathDefinite}.
     */
    public static Integer jsonLength(
            final JsonValueContext parsedInput,
            final String pathSpec,
            final boolean isPathDefinite) {
        // An empty path is ruled out up front because JsonPath rejects it with an
        // IllegalArgumentException instead of the InvalidPathException caught below.
        if (parsedInput == null || parsedInput.hasException() || pathSpec.isEmpty()) {
            return null;
        }

        // JsonPath rejects a null root document, so a whole document that is a JSON null literal
        // has to be resolved here. Only the root path matches it, as a scalar of length 1.
        if (parsedInput.obj == null) {
            return "$".equals(pathSpec) ? 1 : null;
        }
        final Object value;
        try {
            value = JsonPath.parse(parsedInput.obj, JSON_PATH_LENGTH_CONFIG).read(pathSpec);
        } catch (InvalidPathException e) {
            // The path does not exist, or is not a valid path at all.
            return null;
        }

        if (!isPathDefinite) {
            final List<?> matched = (List<?>) value;
            return matched.size() == 1 ? jsonLengthValue(matched.get(0)) : null;
        }

        // A definite path that read without throwing but produced null matched a JSON null
        // literal, which jsonLengthValue counts as a scalar.
        return jsonLengthValue(value);
    }

    private static int jsonLengthValue(final Object value) {
        if (value instanceof Map) {
            return ((Map<?, ?>) value).size();
        } else if (value != null && value.getClass().isArray()) {
            return Array.getLength(value);
        } else if (value instanceof List<?>) {
            return ((List<?>) value).size();
        }

        // Scalars, including a JSON null literal, have length 1.
        return 1;
    }

    public static Object json(String input) {
        try {
            String trimmed = input.trim();
            if (trimmed.isEmpty()) {
                return null;
            }
            String jsonStr = jsonize(dejsonize(trimmed));

            return jsonStr;
        } catch (Exception e) {
            throw new TableRuntimeException(
                    String.format(
                            "Invalid JSON string in JSON(value) function: \"%s\". Error: %s",
                            input, e.getMessage()),
                    e);
        }
    }

    public static boolean isJsonValue(String input) {
        try {
            dejsonize(input);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isJsonObject(String input) {
        try {
            Object o = dejsonize(input);
            return o instanceof Map;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isJsonArray(String input) {
        try {
            Object o = dejsonize(input);
            return o instanceof Collection;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean isJsonScalar(String input) {
        try {
            Object o = dejsonize(input);
            return !(o instanceof Map) && !(o instanceof Collection);
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isScalarObject(Object obj) {
        if (obj instanceof Collection) {
            return false;
        }
        if (obj instanceof Map) {
            return false;
        }
        return true;
    }

    private static String jsonize(Object input) {
        return JSON_PATH_JSON_PROVIDER.toJson(input);
    }

    private static Object dejsonize(String input) {
        return JSON_PATH_JSON_PROVIDER.parse(input);
    }

    /**
     * Returns the JSON type flag for the parsed value: {@code object}, {@code array}, {@code
     * string}, {@code number}, {@code boolean}, or {@code null} for the JSON null literal. Returns
     * SQL {@code NULL} for invalid JSON.
     */
    public static String jsonType(final JsonValueContext parsedInput) {
        // Unparsed, or shared with a call that never assigned it: report NULL either way.
        if (parsedInput == null || parsedInput.hasException()) {
            return null;
        }
        return getJsonType(parsedInput.obj);
    }

    private static String getJsonType(final Object val) {
        if (val instanceof Number) {
            return "number";
        } else if (val instanceof String) {
            return "string";
        } else if (val instanceof Boolean) {
            return "boolean";
        } else if (val instanceof Map) {
            return "object";
        } else if (val instanceof Collection) {
            return "array";
        } else if (val == null) {
            return "null";
        }
        return null;
    }

    /**
     * Returns the JSON type flag at {@code path}, or {@code null} if the path doesn't resolve to
     * exactly one value. {@code definite} is computed at plan time by {@link #isPathDefinite}.
     */
    public static String jsonType(
            final JsonValueContext parsedInput, final String path, final boolean isPathDefinite) {
        if (parsedInput == null || parsedInput.hasException() || path.isEmpty()) {
            return null;
        }

        if (parsedInput.obj == null) {
            return "$".equals(path) ? "null" : null;
        }

        final Object value;
        try {
            // PathNotFoundException extends InvalidPathException (covers both exceptions)
            value = JsonPath.parse(parsedInput.obj, JSON_PATH_TYPE_CONFIG).read(path);
        } catch (InvalidPathException e) {
            return null;
        }

        if (!isPathDefinite) {
            // Indefinite paths (e.g. wildcards) read back as a list; only one match has one type.
            final List<?> matched = (List<?>) value;
            return matched.size() == 1 ? getJsonType(matched.get(0)) : null;
        }
        return getJsonType(value);
    }

    /** Returns whether {@code pathSpec} is a definite JSON path. */
    public static boolean isPathDefinite(final String pathSpec) {
        // JsonPath.compile() rejects an empty path with an IllegalArgumentException rather than
        // the InvalidPathException caught below.
        if (pathSpec.isEmpty()) {
            return false;
        }
        try {
            return JsonPath.isPathDefinite(pathSpec);
        } catch (InvalidPathException e) {
            return false;
        }
    }

    /**
     * Parses a JSON string into a reusable context object. The result can be passed to {@link
     * #jsonValue} or {@link #jsonQueryParsed} to avoid re-parsing the same JSON string multiple
     * times.
     */
    public static JsonValueContext jsonParse(String input) {
        return jsonValueExpression(input);
    }

    private static JsonValueContext jsonValueExpression(String input) {
        try {
            return JsonValueContext.withJavaObj(dejsonize(input));
        } catch (Exception e) {
            return JsonValueContext.withException(e);
        }
    }

    private static JsonPathContext jsonApiCommonSyntax(String input, String pathSpec) {
        return jsonApiCommonSyntax(jsonValueExpression(input), pathSpec);
    }

    private static JsonPathContext jsonApiCommonSyntax(JsonValueContext input, String pathSpec) {
        PathMode mode;
        String pathStr;
        try {
            Matcher matcher = JSON_PATH_BASE.matcher(pathSpec);
            if (!matcher.matches()) {
                mode = PathMode.STRICT;
                pathStr = pathSpec;
            } else {
                mode = PathMode.valueOf(matcher.group(1).toUpperCase(Locale.ROOT));
                pathStr = matcher.group(2);
            }
            DocumentContext ctx;
            switch (mode) {
                case STRICT:
                    if (input.hasException()) {
                        return JsonPathContext.withStrictException(pathSpec, input.exc);
                    }
                    ctx =
                            JsonPath.parse(
                                    input.obj,
                                    Configuration.builder()
                                            .jsonProvider(JSON_PATH_JSON_PROVIDER)
                                            .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                                            .build());
                    break;
                case LAX:
                    if (input.hasException()) {
                        return JsonPathContext.withJavaObj(PathMode.LAX, null);
                    }
                    ctx =
                            JsonPath.parse(
                                    input.obj,
                                    Configuration.builder()
                                            .options(Option.SUPPRESS_EXCEPTIONS)
                                            .jsonProvider(JSON_PATH_JSON_PROVIDER)
                                            .mappingProvider(JSON_PATH_MAPPING_PROVIDER)
                                            .build());
                    break;
                default:
                    throw illegalJsonPathModeInPathSpec(mode.toString(), pathSpec);
            }
            try {
                return JsonPathContext.withJavaObj(mode, ctx.read(pathStr));
            } catch (Exception e) {
                return JsonPathContext.withStrictException(pathSpec, e);
            }
        } catch (Exception e) {
            return JsonPathContext.withUnknownException(e);
        }
    }

    private static TableRuntimeException toUnchecked(Exception e) {
        if (e instanceof TableRuntimeException) {
            return (TableRuntimeException) e;
        }
        return new TableRuntimeException(e.getMessage(), e);
    }

    private static RuntimeException illegalJsonPathModeInPathSpec(
            String pathMode, String pathSpec) {
        return new TableRuntimeException(
                String.format(
                        "Illegal jsonpath mode ''%s'' in jsonpath spec: ''%s''",
                        pathMode, pathSpec));
    }

    private static RuntimeException illegalJsonPathMode(String pathMode) {
        return new TableRuntimeException(String.format("Illegal jsonpath mode ''%s''", pathMode));
    }

    private static RuntimeException illegalJsonPathSpec(String pathSpec) {
        return new TableRuntimeException(
                String.format(
                        "Illegal jsonpath spec ''%s'', format of the spec should be: ''<lax|strict> $'{'expr'}'''",
                        pathSpec));
    }

    private static RuntimeException strictPathModeRequiresNonEmptyValue() {
        return new TableRuntimeException(
                "Strict jsonpath mode requires a non empty returned value, but is null");
    }

    private static RuntimeException emptyResultOfJsonValueFuncNotAllowed() {
        return new TableRuntimeException("Empty result of JSON_VALUE function is not allowed");
    }

    private static RuntimeException illegalEmptyBehaviorFunc(
            String emptyBehavior, String functionName) {
        return new TableRuntimeException(
                String.format(
                        "Illegal empty behavior ''{0}'' specified in %s function",
                        emptyBehavior, functionName));
    }

    private static RuntimeException illegalErrorBehaviorFunc(
            String errorBehavior, String functionName) {
        return new TableRuntimeException(
                String.format(
                        "Illegal error behavior ''%s'' specified in %s function",
                        errorBehavior, functionName));
    }

    private static RuntimeException scalarValueRequiredInStrictModeOfJsonValueFunc(String value) {
        return new TableRuntimeException(
                String.format(
                        "Strict jsonpath mode requires scalar value, and the actual value is: ''%s''",
                        value));
    }

    private static RuntimeException illegalWrapperBehaviorInJsonQueryFunc(String wrapperBehavior) {
        return new TableRuntimeException(
                String.format(
                        "Illegal wrapper behavior ''%s'' specified in JSON_QUERY function",
                        wrapperBehavior));
    }

    private static RuntimeException emptyResultOfJsonQueryFuncNotAllowed() {
        return new TableRuntimeException("Empty result of JSON_QUERY function is not allowed");
    }

    private static RuntimeException arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(
            String value) {
        return new TableRuntimeException(
                String.format(
                        "Strict jsonpath mode requires array or object value, and the actual value is: ''%s''",
                        value));
    }

    /**
     * Path spec has two different modes: lax mode and strict mode. Lax mode suppresses any thrown
     * exception and returns null, whereas strict mode throws exceptions.
     */
    public enum PathMode {
        LAX,
        STRICT,
        UNKNOWN,
        NONE
    }

    /** Returned path context of JsonApiCommonSyntax, public for testing. */
    private static class JsonPathContext {
        public final PathMode mode;
        public final Object obj;
        public final Exception exc;

        private JsonPathContext(Object obj, Exception exc) {
            this(PathMode.NONE, obj, exc);
        }

        private JsonPathContext(PathMode mode, Object obj, Exception exc) {
            assert obj == null || exc == null;
            this.mode = mode;
            this.obj = obj;
            this.exc = exc;
        }

        public boolean hasException() {
            return exc != null;
        }

        public static JsonPathContext withUnknownException(Exception exc) {
            return new JsonPathContext(PathMode.UNKNOWN, null, exc);
        }

        public static JsonPathContext withStrictException(Exception exc) {
            return new JsonPathContext(PathMode.STRICT, null, exc);
        }

        public static JsonPathContext withStrictException(String pathSpec, Exception exc) {
            if (exc.getClass() == InvalidPathException.class) {
                exc = illegalJsonPathSpec(pathSpec);
            }
            return withStrictException(exc);
        }

        public static JsonPathContext withJavaObj(PathMode mode, Object obj) {
            if (mode == PathMode.UNKNOWN) {
                throw illegalJsonPathMode(mode.toString());
            }
            if (mode == PathMode.STRICT && obj == null) {
                throw strictPathModeRequiresNonEmptyValue();
            }
            return new JsonPathContext(mode, obj, null);
        }

        @Override
        public String toString() {
            return "JsonPathContext{" + "mode=" + mode + ", obj=" + obj + ", exc=" + exc + '}';
        }
    }

    // --- Type conversion for JSON_VALUE / JSON_QUERY RETURNING ---

    private static final java.util.EnumSet<LogicalTypeRoot> SUPPORTED_JSON_RETURNING_TYPES =
            java.util.EnumSet.of(
                    LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.TINYINT,
                    LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER,
                    LogicalTypeRoot.BIGINT,
                    LogicalTypeRoot.FLOAT,
                    LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.DECIMAL);

    public static boolean isSupportedJsonReturningType(LogicalTypeRoot typeRoot) {
        return SUPPORTED_JSON_RETURNING_TYPES.contains(typeRoot);
    }

    public static Object convertJsonScalar(
            Object raw,
            LogicalTypeRoot typeRoot,
            int precision,
            int scale,
            JsonValueOnEmptyOrError errorBehavior,
            Object defaultValue) {
        if (raw == null) {
            return null;
        }
        try {
            return convertToType(raw, typeRoot, precision, scale);
        } catch (JsonConversionException e) {
            switch (errorBehavior) {
                case NULL:
                    return null;
                case DEFAULT:
                    return convertDefault(defaultValue, typeRoot, precision, scale);
                case ERROR:
                    throw new TableRuntimeException(
                            "Cannot cast " + raw.getClass().getName() + " to " + typeRoot, e);
                default:
                    throw new TableRuntimeException(
                            "Unreachable: unknown error behavior " + errorBehavior);
            }
        }
    }

    public static GenericArrayData convertJsonArray(
            Object rawResult,
            LogicalTypeRoot elementTypeRoot,
            int precision,
            int scale,
            JsonQueryOnEmptyOrError errorBehavior) {
        if (rawResult == null) {
            return null;
        }
        try {
            Object[] rawArr = (Object[]) rawResult;
            Object[] converted = new Object[rawArr.length];
            for (int i = 0; i < rawArr.length; i++) {
                if (rawArr[i] != null) {
                    converted[i] = convertToType(rawArr[i], elementTypeRoot, precision, scale);
                }
            }
            return new GenericArrayData(converted);
        } catch (JsonConversionException e) {
            switch (errorBehavior) {
                case NULL:
                    return null;
                case EMPTY_ARRAY:
                    return new GenericArrayData(new Object[0]);
                case ERROR:
                    throw new TableRuntimeException("Array element type mismatch in JSON_QUERY", e);
                default:
                    return null;
            }
        }
    }

    private static Object convertToType(
            Object raw, LogicalTypeRoot typeRoot, int precision, int scale) {
        if (raw instanceof StringData) {
            return convertToType(raw.toString(), typeRoot, precision, scale);
        }
        if (raw instanceof String) {
            if (typeRoot == LogicalTypeRoot.BOOLEAN) {
                return parseStringAsBoolean((String) raw);
            }
            try {
                return convertToType(new BigDecimal((String) raw), typeRoot, precision, scale);
            } catch (NumberFormatException e) {
                throw new JsonConversionException(
                        "Cannot parse string '" + raw + "' as " + typeRoot, e);
            }
        }
        try {
            switch (typeRoot) {
                case BOOLEAN:
                    if (raw instanceof Number) {
                        int v = ((Number) raw).intValue();
                        if (v == 0) return false;
                        if (v == 1) return true;
                        throw new JsonConversionException(
                                "Cannot convert " + raw + " to BOOLEAN");
                    }
                    return (Boolean) raw;
                case TINYINT:
                    return toCheckedByte((Number) raw);
                case SMALLINT:
                    return toCheckedShort((Number) raw);
                case INTEGER:
                    return toCheckedInt((Number) raw);
                case BIGINT:
                    return toCheckedLong((Number) raw);
                case FLOAT:
                    return toCheckedFloat((Number) raw);
                case DOUBLE:
                    return toCheckedDouble((Number) raw);
                case DECIMAL:
                    return toCheckedDecimal(raw.toString(), precision, scale);
                default:
                    throw new JsonConversionException(
                            "Unsupported type for JSON conversion: " + typeRoot);
            }
        } catch (ClassCastException e) {
            throw new JsonConversionException(
                    "Cannot convert " + raw.getClass().getName() + " to " + typeRoot, e);
        }
    }

    private static Object convertDefault(
            Object defaultValue, LogicalTypeRoot typeRoot, int precision, int scale) {
        if (defaultValue == null) {
            return null;
        }
        try {
            return convertToType(defaultValue, typeRoot, precision, scale);
        } catch (JsonConversionException e) {
            throw new TableRuntimeException(
                    "Default value " + defaultValue + " cannot be represented as " + typeRoot, e);
        }
    }

    private static @Nullable BigInteger toBigIntegerTruncated(Number n) {
        if (n instanceof BigDecimal) {
            return ((BigDecimal) n).toBigInteger();
        }
        if (n instanceof BigInteger) {
            return (BigInteger) n;
        }
        return null;
    }

    static byte toCheckedByte(Number n) {
        BigInteger bi = toBigIntegerTruncated(n);
        if (bi != null) {
            if (bi.compareTo(BigInteger.valueOf(Byte.MAX_VALUE)) > 0
                    || bi.compareTo(BigInteger.valueOf(Byte.MIN_VALUE)) < 0) {
                throw new JsonConversionException("Value " + n + " is out of range for TINYINT");
            }
            return bi.byteValue();
        }
        long v = n.longValue();
        if (v < Byte.MIN_VALUE || v > Byte.MAX_VALUE) {
            throw new JsonConversionException("Value " + n + " is out of range for TINYINT");
        }
        return (byte) v;
    }

    static short toCheckedShort(Number n) {
        BigInteger bi = toBigIntegerTruncated(n);
        if (bi != null) {
            if (bi.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0
                    || bi.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0) {
                throw new JsonConversionException("Value " + n + " is out of range for SMALLINT");
            }
            return bi.shortValue();
        }
        long v = n.longValue();
        if (v < Short.MIN_VALUE || v > Short.MAX_VALUE) {
            throw new JsonConversionException("Value " + n + " is out of range for SMALLINT");
        }
        return (short) v;
    }

    static int toCheckedInt(Number n) {
        BigInteger bi = toBigIntegerTruncated(n);
        if (bi != null) {
            if (bi.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0
                    || bi.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0) {
                throw new JsonConversionException("Value " + n + " is out of range for INTEGER");
            }
            return bi.intValue();
        }
        long v = n.longValue();
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
            throw new JsonConversionException("Value " + n + " is out of range for INTEGER");
        }
        return (int) v;
    }

    static boolean parseStringAsBoolean(String s) {
        String v = s.trim().toLowerCase(Locale.ROOT);
        switch (v) {
            case "true":
            case "t":
            case "yes":
            case "1":
                return true;
            case "false":
            case "f":
            case "no":
            case "0":
                return false;
            default:
                throw new JsonConversionException("Cannot parse string '" + s + "' as BOOLEAN");
        }
    }

    static float toCheckedFloat(Number n) {
        float f = n.floatValue();
        if (Float.isInfinite(f) || Float.isNaN(f)) {
            throw new JsonConversionException("Value " + n + " is out of range for FLOAT");
        }
        return f;
    }

    static double toCheckedDouble(Number n) {
        double d = n.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            throw new JsonConversionException("Value " + n + " is out of range for DOUBLE");
        }
        return d;
    }

    static long toCheckedLong(Number n) {
        BigInteger bi = toBigIntegerTruncated(n);
        if (bi != null) {
            if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0
                    || bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
                throw new JsonConversionException("Value " + n + " is out of range for BIGINT");
            }
            return bi.longValue();
        }
        return n.longValue();
    }

    static DecimalData toCheckedDecimal(String value, int precision, int scale) {
        DecimalData result = DecimalDataUtils.castFrom(value, precision, scale);
        if (result == null) {
            throw new JsonConversionException(
                    "Value "
                            + value
                            + " cannot be represented as DECIMAL("
                            + precision
                            + ", "
                            + scale
                            + ")");
        }
        return result;
    }

    public static class JsonValueContext {
        @JsonValue public final Object obj;
        public final Exception exc;

        private JsonValueContext(Object obj, Exception exc) {
            assert obj == null || exc == null;
            this.obj = obj;
            this.exc = exc;
        }

        public static JsonValueContext withJavaObj(Object obj) {
            return new JsonValueContext(obj, null);
        }

        public static JsonValueContext withException(Exception exc) {
            return new JsonValueContext(null, exc);
        }

        public boolean hasException() {
            return exc != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JsonValueContext jsonValueContext = (JsonValueContext) o;
            return Objects.equals(obj, jsonValueContext.obj);
        }

        @Override
        public int hashCode() {
            return Objects.hash(obj);
        }

        @Override
        public String toString() {
            return Objects.toString(obj);
        }
    }
}
