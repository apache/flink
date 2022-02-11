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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.runtime.groupwindow.WindowReference;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.Optional;

/** An utility class that provide abilities for JSON serialization and deserialization. */
public class JsonSerdeUtil {

    /** Return true if the given class's constructors have @JsonCreator annotation, else false. */
    public static boolean hasJsonCreatorAnnotation(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            for (Annotation annotation : constructor.getAnnotations()) {
                if (annotation instanceof JsonCreator) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Object mapper shared instance to serialize and deserialize the plan. Note that creating and
     * copying of object mappers is expensive and should be avoided.
     */
    private static final ObjectMapper OBJECT_MAPPER_INSTANCE;

    static {
        OBJECT_MAPPER_INSTANCE = new ObjectMapper();

        OBJECT_MAPPER_INSTANCE.setTypeFactory(
                // Make sure to register the classloader of the planner
                OBJECT_MAPPER_INSTANCE
                        .getTypeFactory()
                        .withClassLoader(JsonSerdeUtil.class.getClassLoader()));
        OBJECT_MAPPER_INSTANCE.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
        OBJECT_MAPPER_INSTANCE.configure(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS, false);
        OBJECT_MAPPER_INSTANCE.registerModule(new Jdk8Module().configureAbsentsAsNulls(true));
        OBJECT_MAPPER_INSTANCE.registerModule(new JavaTimeModule());
        OBJECT_MAPPER_INSTANCE.registerModule(createFlinkTableJacksonModule());
    }

    /** Get the {@link ObjectMapper} instance. */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER_INSTANCE;
    }

    public static ObjectReader createObjectReader(SerdeContext serdeContext) {
        return OBJECT_MAPPER_INSTANCE
                .reader()
                .withAttribute(SerdeContext.SERDE_CONTEXT_KEY, serdeContext);
    }

    public static ObjectWriter createObjectWriter(SerdeContext serdeContext) {
        return OBJECT_MAPPER_INSTANCE
                .writer()
                .withAttribute(SerdeContext.SERDE_CONTEXT_KEY, serdeContext);
    }

    private static Module createFlinkTableJacksonModule() {
        final SimpleModule module = new SimpleModule("Flink table module");
        ExecNodeMetadataUtil.execNodes().stream()
                .forEach(c -> module.registerSubtypes(new NamedType(c, c.getName())));
        registerSerializers(module);
        registerDeserializers(module);
        registerMixins(module);

        return module;
    }

    private static void registerSerializers(SimpleModule module) {
        module.addSerializer(new ExecNodeGraphJsonSerializer());
        // ObjectIdentifierJsonSerializer is needed for LogicalType serialization
        module.addSerializer(new ObjectIdentifierJsonSerializer());
        // LogicalTypeJsonSerializer is needed for RelDataType serialization
        module.addSerializer(new LogicalTypeJsonSerializer());
        // DataTypeJsonSerializer is needed for LogicalType serialization
        module.addSerializer(new DataTypeJsonSerializer());
        // RelDataTypeJsonSerializer is needed for RexNode serialization
        module.addSerializer(new RelDataTypeJsonSerializer());
        // RexNode is used in many exec nodes, so we register its serializer directly here
        module.addSerializer(new RexNodeJsonSerializer());
        module.addSerializer(new AggregateCallJsonSerializer());
        module.addSerializer(new ChangelogModeJsonSerializer());
        module.addSerializer(new LogicalWindowJsonSerializer());
        module.addSerializer(new RexWindowBoundJsonSerializer());
        module.addSerializer(new WindowReferenceJsonSerializer());
        module.addSerializer(new ContextResolvedTableJsonSerializer());
        module.addSerializer(new ColumnJsonSerializer());
        module.addSerializer(new ResolvedCatalogTableJsonSerializer());
        module.addSerializer(new ResolvedExpressionJsonSerializer());
        module.addSerializer(new ResolvedSchemaJsonSerializer());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void registerDeserializers(SimpleModule module) {
        module.addDeserializer(ExecNodeGraph.class, new ExecNodeGraphJsonDeserializer());
        // ObjectIdentifierJsonDeserializer is needed for LogicalType deserialization
        module.addDeserializer(ObjectIdentifier.class, new ObjectIdentifierJsonDeserializer());
        // LogicalTypeJsonSerializer is needed for RelDataType serialization
        module.addDeserializer(LogicalType.class, new LogicalTypeJsonDeserializer());
        // DataTypeJsonDeserializer is needed for LogicalType serialization
        module.addDeserializer(DataType.class, new DataTypeJsonDeserializer());
        // RelDataTypeJsonSerializer is needed for RexNode serialization
        module.addDeserializer(RelDataType.class, new RelDataTypeJsonDeserializer());
        // RexNode is used in many exec nodes, so we register its deserializer directly here
        module.addDeserializer(RexNode.class, new RexNodeJsonDeserializer());
        // We need this explicit mapping to make sure Jackson can deserialize POJOs declaring fields
        // with RexLiteral instead of RexNode.
        module.addDeserializer(RexLiteral.class, (StdDeserializer) new RexNodeJsonDeserializer());
        module.addDeserializer(AggregateCall.class, new AggregateCallJsonDeserializer());
        module.addDeserializer(ChangelogMode.class, new ChangelogModeJsonDeserializer());
        module.addDeserializer(LogicalWindow.class, new LogicalWindowJsonDeserializer());
        module.addDeserializer(RexWindowBound.class, new RexWindowBoundJsonDeserializer());
        module.addDeserializer(WindowReference.class, new WindowReferenceJsonDeserializer());
        module.addDeserializer(
                ContextResolvedTable.class, new ContextResolvedTableJsonDeserializer());
        module.addDeserializer(Column.class, new ColumnJsonDeserializer());
        module.addDeserializer(
                ResolvedCatalogTable.class, new ResolvedCatalogTableJsonDeserializer());
        module.addDeserializer(ResolvedExpression.class, new ResolvedExpressionJsonDeserializer());
        module.addDeserializer(ResolvedSchema.class, new ResolvedSchemaJsonDeserializer());
    }

    private static void registerMixins(SimpleModule module) {
        module.setMixInAnnotation(WatermarkSpec.class, WatermarkSpecMixin.class);
        module.setMixInAnnotation(UniqueConstraint.class, UniqueConstraintMixin.class);
    }

    // Utilities for SerDes implementations

    static JsonParser traverse(TreeNode node, ObjectCodec objectCodec) throws IOException {
        JsonParser jsonParser = node.traverse(objectCodec);
        // https://stackoverflow.com/questions/55855414/custom-jackson-deserialization-getting-com-fasterxml-jackson-databind-exc-mism
        if (!node.isMissingNode()) {
            // Initialize first token
            if (jsonParser.getCurrentToken() == null) {
                jsonParser.nextToken();
            }
        }
        return jsonParser;
    }

    static void serializeOptionalField(
            JsonGenerator jsonGenerator,
            String fieldName,
            Optional<?> value,
            SerializerProvider serializerProvider)
            throws IOException {
        if (value.isPresent()) {
            serializerProvider.defaultSerializeField(fieldName, value.get(), jsonGenerator);
        }
    }

    static <T> Optional<T> deserializeOptionalField(
            ObjectNode objectNode,
            String fieldName,
            Class<T> clazz,
            ObjectCodec codec,
            DeserializationContext ctx)
            throws IOException {
        if (objectNode.hasNonNull(fieldName)) {
            return Optional.ofNullable(
                    ctx.readValue(traverse(objectNode.get(fieldName), codec), clazz));
        }
        return Optional.empty();
    }

    static <T> Optional<T> deserializeOptionalField(
            ObjectNode objectNode,
            String fieldName,
            JavaType type,
            ObjectCodec codec,
            DeserializationContext ctx)
            throws IOException {
        if (objectNode.hasNonNull(fieldName)) {
            return Optional.of(ctx.readValue(traverse(objectNode.get(fieldName), codec), type));
        }
        return Optional.empty();
    }

    private JsonSerdeUtil() {}
}
