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

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.utils.ReflectionsUtil;
import org.apache.flink.table.runtime.groupwindow.WindowReference;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.Module;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype.NamedType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.time.Duration;

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
        ReflectionsUtil.scanSubClasses(
                        "org.apache.flink.table.planner.plan.nodes.exec", ExecNode.class)
                .forEach(c -> module.registerSubtypes(new NamedType(c, c.getName())));
        registerSerializers(module);
        registerDeserializers(module);

        return module;
    }

    private static void registerSerializers(SimpleModule module) {
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
        module.addSerializer(new DurationJsonSerializer());
        module.addSerializer(new ChangelogModeJsonSerializer());
        module.addSerializer(new LogicalWindowJsonSerializer());
        module.addSerializer(new RexWindowBoundJsonSerializer());
        module.addSerializer(new WindowReferenceJsonSerializer());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void registerDeserializers(SimpleModule module) {
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
        module.addDeserializer(Duration.class, new DurationJsonDeserializer());
        module.addDeserializer(ChangelogMode.class, new ChangelogModeJsonDeserializer());
        module.addDeserializer(LogicalWindow.class, new LogicalWindowJsonDeserializer());
        module.addDeserializer(RexWindowBound.class, new RexWindowBoundJsonDeserializer());
        module.addDeserializer(WindowReference.class, new WindowReferenceJsonDeserializer());
    }

    private JsonSerdeUtil() {}
}
