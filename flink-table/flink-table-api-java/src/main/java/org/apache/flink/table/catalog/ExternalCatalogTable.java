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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Helper {@link CatalogTable} for representing a table that is backed by some inline connector
 * (i.e. {@code DataStream} or {@link TableResult#collect()}).
 */
@Internal
public final class ExternalCatalogTable implements CatalogTable {

    private final Schema schema;

    public ExternalCatalogTable(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getOptions() {
        throw new TableException(
                "A catalog table that is backed by a DataStream or used for TableResult.collect() "
                        + "cannot be expressed with options and can thus also not be persisted.");
    }

    @Override
    public String getComment() {
        return "Inline catalog table";
    }

    @Override
    public CatalogBaseTable copy() {
        return new ExternalCatalogTable(schema);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<String> getPartitionKeys() {
        return Collections.emptyList();
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        throw new TableException(
                "A catalog table that is backed by a DataStream or used for TableResult.collect() "
                        + "cannot be expressed with options and can thus also not be enriched "
                        + "with hints.");
    }
}
