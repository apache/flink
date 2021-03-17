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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;

/**
 * {@link DynamicTableSourceSpec} describes how to serialize/deserialize dynamic table sink table
 * and create {@link DynamicTableSink} from the deserialization result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class DynamicTableSinkSpec extends CatalogTableSpecBase {

    public static final String FIELD_NAME_SINK_ABILITY_SPECS = "sinkAbilitySpecs";

    @JsonIgnore private DynamicTableSink tableSink;

    @JsonProperty(FIELD_NAME_SINK_ABILITY_SPECS)
    private final @Nullable List<SinkAbilitySpec> sinkAbilitySpecs;

    @JsonCreator
    public DynamicTableSinkSpec(
            @JsonProperty(FIELD_NAME_IDENTIFIER) ObjectIdentifier objectIdentifier,
            @JsonProperty(FIELD_NAME_CATALOG_TABLE) ResolvedCatalogTable catalogTable,
            @Nullable @JsonProperty(FIELD_NAME_SINK_ABILITY_SPECS)
                    List<SinkAbilitySpec> sinkAbilitySpecs) {
        super(objectIdentifier, catalogTable);
        this.sinkAbilitySpecs = sinkAbilitySpecs;
    }

    public DynamicTableSink getTableSink() {
        if (tableSink == null) {
            tableSink =
                    FactoryUtil.createTableSink(
                            null, // catalog, TODO support create Factory from catalog
                            objectIdentifier,
                            catalogTable,
                            configuration,
                            classLoader,
                            // isTemporary, it's always true since the catalog is always null now.
                            true);
            if (sinkAbilitySpecs != null) {
                sinkAbilitySpecs.forEach(spec -> spec.apply(tableSink));
            }
        }
        return tableSink;
    }

    public void setTableSink(DynamicTableSink tableSink) {
        this.tableSink = tableSink;
    }
}
