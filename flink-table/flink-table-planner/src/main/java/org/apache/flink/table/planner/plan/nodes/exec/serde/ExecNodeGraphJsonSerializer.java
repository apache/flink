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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraphValidator;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

/**
 * JSON serializer for {@link ExecNodeGraph}.
 *
 * @see ExecNodeGraphJsonDeserializer for the reverse operation
 */
@Internal
final class ExecNodeGraphJsonSerializer extends StdSerializer<ExecNodeGraph> {
    private static final long serialVersionUID = 1L;

    ExecNodeGraphJsonSerializer() {
        super(ExecNodeGraph.class);
    }

    @Override
    public void serialize(
            ExecNodeGraph execNodeGraph,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        validate(execNodeGraph);
        serializerProvider.defaultSerializeValue(
                JsonPlanGraph.fromExecNodeGraph(execNodeGraph), jsonGenerator);
    }

    /** Check whether the given {@link ExecNodeGraph} is completely legal. */
    private static void validate(ExecNodeGraph execGraph) {
        ExecNodeVisitor visitor = new ExecNodeGraphValidator();
        execGraph.getRootNodes().forEach(visitor::visit);
    }
}
