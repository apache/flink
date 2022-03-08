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
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import java.util.Collection;
import java.util.Collections;
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
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    private static final Pattern JSON_PATH_BASE =
            Pattern.compile(
                    "^\\s*(?<mode>strict|lax)\\s+(?<spec>.+)$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
    private static final JacksonJsonProvider JSON_PATH_JSON_PROVIDER = new JacksonJsonProvider();
    private static final MappingProvider JSON_PATH_MAPPING_PROVIDER = new JacksonMappingProvider();

    private SqlJsonUtils() {}

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
            throw new TableException("JSON object could not be serialized: " + node.asText(), e);
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
                    throw illegalErrorBehaviorInJsonExistsFunc(errorBehavior.toString());
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
                        throw illegalEmptyBehaviorInJsonValueFunc(emptyBehavior.toString());
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
                throw illegalErrorBehaviorInJsonValueFunc(errorBehavior.toString());
        }
    }

    public static String jsonQuery(
            String input,
            String pathSpec,
            JsonQueryWrapper wrapperBehavior,
            JsonQueryOnEmptyOrError emptyBehavior,
            JsonQueryOnEmptyOrError errorBehavior) {
        return jsonQuery(
                jsonApiCommonSyntax(input, pathSpec),
                wrapperBehavior,
                emptyBehavior,
                errorBehavior);
    }

    private static String jsonQuery(
            JsonPathContext context,
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
                switch (emptyBehavior) {
                    case ERROR:
                        throw emptyResultOfJsonQueryFuncNotAllowed();
                    case NULL:
                        return null;
                    case EMPTY_ARRAY:
                        return "[]";
                    case EMPTY_OBJECT:
                        return "{}";
                    default:
                        throw illegalEmptyBehaviorInJsonQueryFunc(emptyBehavior.toString());
                }
            } else if (context.mode == PathMode.STRICT && isScalarObject(value)) {
                exc = arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(value.toString());
            } else {
                try {
                    return jsonize(value);
                } catch (Exception e) {
                    exc = e;
                }
            }
        }
        switch (errorBehavior) {
            case ERROR:
                throw toUnchecked(exc);
            case NULL:
                return null;
            case EMPTY_ARRAY:
                return "[]";
            case EMPTY_OBJECT:
                return "{}";
            default:
                throw illegalErrorBehaviorInJsonQueryFunc(errorBehavior.toString());
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

    private static RuntimeException toUnchecked(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException(e);
    }

    private static RuntimeException illegalJsonPathModeInPathSpec(
            String pathMode, String pathSpec) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal jsonpath mode ''%s'' in jsonpath spec: ''%s''",
                        pathMode, pathSpec));
    }

    private static RuntimeException illegalJsonPathMode(String pathMode) {
        return new FlinkRuntimeException(String.format("Illegal jsonpath mode ''%s''", pathMode));
    }

    private static RuntimeException illegalJsonPathSpec(String pathSpec) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal jsonpath spec ''%s'', format of the spec should be: ''<lax|strict> $'{'expr'}'''",
                        pathSpec));
    }

    private static RuntimeException strictPathModeRequiresNonEmptyValue() {
        return new FlinkRuntimeException(
                "Strict jsonpath mode requires a non empty returned value, but is null");
    }

    private static RuntimeException illegalErrorBehaviorInJsonExistsFunc(String errorBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal error behavior ''{0}'' specified in JSON_EXISTS function",
                        errorBehavior));
    }

    private static RuntimeException emptyResultOfJsonValueFuncNotAllowed() {
        return new FlinkRuntimeException("Empty result of JSON_VALUE function is not allowed");
    }

    private static RuntimeException illegalEmptyBehaviorInJsonValueFunc(String emptyBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal empty behavior ''{0}'' specified in JSON_VALUE function",
                        emptyBehavior));
    }

    private static RuntimeException illegalErrorBehaviorInJsonValueFunc(String errorBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal error behavior ''%s'' specified in JSON_VALUE function",
                        errorBehavior));
    }

    private static RuntimeException scalarValueRequiredInStrictModeOfJsonValueFunc(String value) {
        return new FlinkRuntimeException(
                String.format(
                        "Strict jsonpath mode requires scalar value, and the actual value is: ''%s''",
                        value));
    }

    private static RuntimeException illegalWrapperBehaviorInJsonQueryFunc(String wrapperBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal wrapper behavior ''%s'' specified in JSON_QUERY function",
                        wrapperBehavior));
    }

    private static RuntimeException emptyResultOfJsonQueryFuncNotAllowed() {
        return new FlinkRuntimeException("Empty result of JSON_QUERY function is not allowed");
    }

    private static RuntimeException illegalEmptyBehaviorInJsonQueryFunc(String emptyBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal empty behavior ''%s'' specified in JSON_VALUE function",
                        emptyBehavior));
    }

    private static RuntimeException arrayOrObjectValueRequiredInStrictModeOfJsonQueryFunc(
            String value) {
        return new FlinkRuntimeException(
                String.format(
                        "Strict jsonpath mode requires array or object value, and the actual value is: ''%s''",
                        value));
    }

    private static RuntimeException illegalErrorBehaviorInJsonQueryFunc(String errorBehavior) {
        return new FlinkRuntimeException(
                String.format(
                        "Illegal error behavior ''%s'' specified in JSON_VALUE function",
                        errorBehavior));
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

    private static class JsonValueContext {
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
