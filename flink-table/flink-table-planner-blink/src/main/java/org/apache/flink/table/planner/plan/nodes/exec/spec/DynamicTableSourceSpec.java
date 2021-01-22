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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link DynamicTableSourceSpec} describes how to serialize/deserialize dynamic table source table
 * and create {@link DynamicTableSource} from the deserialization result.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DynamicTableSourceSpec extends CatalogTableSpecBase {

    @JsonIgnore private DynamicTableSource tableSource;

    @JsonCreator
    public DynamicTableSourceSpec(
            @JsonProperty(FIELD_NAME_IDENTIFIER) ObjectIdentifier objectIdentifier,
            @JsonProperty(FIELD_NAME_CATALOG_TABLE) CatalogTable catalogTable) {
        super(objectIdentifier, catalogTable);
    }

    @JsonIgnore
    private DynamicTableSource getTableSource() {
        checkNotNull(configuration);
        if (tableSource == null) {
            tableSource =
                    FactoryUtil.createTableSource(
                            null, // catalog, TODO support create Factory from catalog
                            objectIdentifier,
                            catalogTable,
                            configuration,
                            classLoader,
                            // isTemporary, it's always true since the catalog is always null now.
                            true);
        }
        return tableSource;
    }

    @JsonIgnore
    public ScanTableSource getScanTableSource() {
        DynamicTableSource tableSource = getTableSource();
        if (tableSource instanceof ScanTableSource) {
            return (ScanTableSource) tableSource;
        } else {
            throw new TableException(
                    String.format(
                            "%s is not a ScanTableSource.\nplease check it.",
                            tableSource.getClass().getName()));
        }
    }

    @JsonIgnore
    public LookupTableSource getLookupTableSource() {
        DynamicTableSource tableSource = getTableSource();
        if (tableSource instanceof LookupTableSource) {
            return (LookupTableSource) tableSource;
        } else {
            throw new TableException(
                    String.format(
                            "%s is not a LookupTableSource.\nplease check it.",
                            tableSource.getClass().getName()));
        }
    }

    public void setTableSource(DynamicTableSource tableSource) {
        this.tableSource = tableSource;
    }
}
