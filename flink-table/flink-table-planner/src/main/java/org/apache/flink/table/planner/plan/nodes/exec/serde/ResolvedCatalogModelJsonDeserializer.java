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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogModel;
import org.apache.flink.table.catalog.DefaultResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedCatalogModel;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeFieldOrNull;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeMapOrEmpty;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.traverse;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogModelJsonSerializer.COMMENT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogModelJsonSerializer.INPUT_SCHEMA;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogModelJsonSerializer.OPTIONS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogModelJsonSerializer.OUTPUT_SCHEMA;

/**
 * JSON deserializer for {@link ResolvedCatalogModel}.
 *
 * @see ResolvedCatalogModelJsonSerializer for the reverse operation
 */
@Internal
public class ResolvedCatalogModelJsonDeserializer extends StdDeserializer<ResolvedCatalogModel> {

    private static final long serialVersionUID = 1L;

    public ResolvedCatalogModelJsonDeserializer() {
        super(ResolvedCatalogModel.class);
    }

    @Override
    public ResolvedCatalogModel deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        ObjectCodec codec = jsonParser.getCodec();

        ResolvedSchema inputSchema =
                ctx.readValue(
                        traverse(jsonNode.required(INPUT_SCHEMA), codec), ResolvedSchema.class);
        ResolvedSchema outputSchema =
                ctx.readValue(
                        traverse(jsonNode.required(OUTPUT_SCHEMA), codec), ResolvedSchema.class);
        String comment = deserializeFieldOrNull(jsonNode, COMMENT, String.class, codec, ctx);
        Map<String, String> options =
                deserializeMapOrEmpty(jsonNode, OPTIONS, String.class, String.class, codec, ctx);

        return new DefaultResolvedCatalogModel(
                CatalogModel.of(
                        Schema.newBuilder().fromResolvedSchema(inputSchema).build(),
                        Schema.newBuilder().fromResolvedSchema(outputSchema).build(),
                        options,
                        comment),
                inputSchema,
                outputSchema);
    }
}
