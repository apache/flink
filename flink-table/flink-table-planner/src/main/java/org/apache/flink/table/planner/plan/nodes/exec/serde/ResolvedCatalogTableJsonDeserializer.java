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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeFieldOrNull;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeListOrEmpty;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.deserializeMapOrEmpty;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.CompiledPlanSerdeUtil.traverse;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.COMMENT;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.DISTRIBUTION;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.OPTIONS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.PARTITION_KEYS;
import static org.apache.flink.table.planner.plan.nodes.exec.serde.ResolvedCatalogTableJsonSerializer.RESOLVED_SCHEMA;

/**
 * JSON deserializer for {@link ResolvedCatalogTable}.
 *
 * @see ResolvedCatalogTableJsonSerializer for the reverse operation
 */
@Internal
final class ResolvedCatalogTableJsonDeserializer extends StdDeserializer<ResolvedCatalogTable> {
    private static final long serialVersionUID = 1L;

    ResolvedCatalogTableJsonDeserializer() {
        super(ResolvedCatalogTable.class);
    }

    @Override
    public ResolvedCatalogTable deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        ObjectNode jsonNode = jsonParser.readValueAsTree();
        ObjectCodec codec = jsonParser.getCodec();

        ResolvedSchema resolvedSchema =
                ctx.readValue(
                        traverse(jsonNode.required(RESOLVED_SCHEMA), codec), ResolvedSchema.class);
        TableDistribution distribution =
                deserializeFieldOrNull(jsonNode, DISTRIBUTION, TableDistribution.class, codec, ctx);
        List<String> partitionKeys =
                deserializeListOrEmpty(jsonNode, PARTITION_KEYS, String.class, codec, ctx);
        String comment = deserializeFieldOrNull(jsonNode, COMMENT, String.class, codec, ctx);
        Map<String, String> options =
                deserializeMapOrEmpty(jsonNode, OPTIONS, String.class, String.class, codec, ctx);

        return new ResolvedCatalogTable(
                // Create the unresolved schema from the resolved one. We do this for safety
                // reason, in case one tries to access the unresolved schema.
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment(comment)
                        .distribution(distribution)
                        .partitionKeys(partitionKeys)
                        .options(options)
                        .build(),
                resolvedSchema);
    }
}
