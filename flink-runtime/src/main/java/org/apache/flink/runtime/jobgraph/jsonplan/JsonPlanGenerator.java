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

package org.apache.flink.runtime.jobgraph.jsonplan;

import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.commons.lang3.StringEscapeUtils;

import java.io.StringWriter;
import java.util.List;

public class JsonPlanGenerator {

    private static final String NOT_SET = "";
    private static final String EMPTY = "{}";

    public static String generatePlan(JobGraph jg) {
        try {
            final StringWriter writer = new StringWriter(1024);

            final JsonFactory factory = new JsonFactory();
            final JsonGenerator gen = factory.createGenerator(writer);

            // start of everything
            gen.writeStartObject();
            gen.writeStringField("jid", jg.getJobID().toString());
            gen.writeStringField("name", jg.getName());
            gen.writeStringField("type", jg.getJobType().name());
            gen.writeArrayFieldStart("nodes");

            // info per vertex
            for (JobVertex vertex : jg.getVertices()) {

                String operator =
                        vertex.getOperatorName() != null ? vertex.getOperatorName() : NOT_SET;

                String operatorDescr =
                        vertex.getOperatorDescription() != null
                                ? vertex.getOperatorDescription()
                                : NOT_SET;

                String optimizerProps =
                        vertex.getResultOptimizerProperties() != null
                                ? vertex.getResultOptimizerProperties()
                                : EMPTY;

                String description =
                        vertex.getOperatorPrettyName() != null
                                ? vertex.getOperatorPrettyName()
                                : vertex.getName();

                // make sure the encoding is HTML pretty
                description = StringEscapeUtils.escapeHtml4(description);
                description = description.replace("\n", "<br/>");
                description = description.replace("\\", "&#92;");

                operatorDescr = StringEscapeUtils.escapeHtml4(operatorDescr);
                operatorDescr = operatorDescr.replace("\n", "<br/>");

                gen.writeStartObject();

                // write the core properties
                gen.writeStringField("id", vertex.getID().toString());
                gen.writeNumberField("parallelism", vertex.getParallelism());
                gen.writeStringField("operator", operator);
                gen.writeStringField("operator_strategy", operatorDescr);
                gen.writeStringField("description", description);

                if (!vertex.isInputVertex()) {
                    // write the input edge properties
                    gen.writeArrayFieldStart("inputs");

                    List<JobEdge> inputs = vertex.getInputs();
                    for (int inputNum = 0; inputNum < inputs.size(); inputNum++) {
                        JobEdge edge = inputs.get(inputNum);
                        if (edge.getSource() == null) {
                            continue;
                        }

                        JobVertex predecessor = edge.getSource().getProducer();

                        String shipStrategy = edge.getShipStrategyName();
                        String preProcessingOperation = edge.getPreProcessingOperationName();
                        String operatorLevelCaching = edge.getOperatorLevelCachingDescription();

                        gen.writeStartObject();
                        gen.writeNumberField("num", inputNum);
                        gen.writeStringField("id", predecessor.getID().toString());

                        if (shipStrategy != null) {
                            gen.writeStringField("ship_strategy", shipStrategy);
                        }
                        if (preProcessingOperation != null) {
                            gen.writeStringField("local_strategy", preProcessingOperation);
                        }
                        if (operatorLevelCaching != null) {
                            gen.writeStringField("caching", operatorLevelCaching);
                        }

                        gen.writeStringField(
                                "exchange", edge.getSource().getResultType().name().toLowerCase());

                        gen.writeEndObject();
                    }

                    gen.writeEndArray();
                }

                // write the optimizer properties
                gen.writeFieldName("optimizer_properties");
                gen.writeRawValue(optimizerProps);

                gen.writeEndObject();
            }

            // end of everything
            gen.writeEndArray();
            gen.writeEndObject();

            gen.close();

            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate plan", e);
        }
    }
}
