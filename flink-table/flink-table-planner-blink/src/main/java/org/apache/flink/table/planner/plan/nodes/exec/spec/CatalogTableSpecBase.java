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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CatalogTableJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.CatalogTableJsonSerializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ObjectIdentifierJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.ObjectIdentifierJsonSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link CatalogTableSpecBase} describes how to serialize/deserialize a catalog table. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogTableSpecBase {
    public static final String FIELD_NAME_IDENTIFIER = "identifier";
    public static final String FIELD_NAME_CATALOG_TABLE = "catalogTable";

    @JsonProperty(value = FIELD_NAME_IDENTIFIER, required = true)
    @JsonSerialize(using = ObjectIdentifierJsonSerializer.class)
    @JsonDeserialize(using = ObjectIdentifierJsonDeserializer.class)
    protected final ObjectIdentifier objectIdentifier;

    @JsonProperty(value = FIELD_NAME_CATALOG_TABLE, required = true)
    @JsonSerialize(using = CatalogTableJsonSerializer.class)
    @JsonDeserialize(using = CatalogTableJsonDeserializer.class)
    protected final ResolvedCatalogTable catalogTable;

    @JsonIgnore protected ClassLoader classLoader;

    @JsonIgnore protected ReadableConfig configuration;

    protected CatalogTableSpecBase(
            ObjectIdentifier objectIdentifier, ResolvedCatalogTable catalogTable) {
        this.objectIdentifier = checkNotNull(objectIdentifier);
        this.catalogTable = checkNotNull(catalogTable);
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public void setReadableConfig(ReadableConfig config) {
        this.configuration = config;
    }

    @JsonIgnore
    public ObjectIdentifier getObjectIdentifier() {
        return objectIdentifier;
    }

    @JsonIgnore
    public ResolvedCatalogTable getCatalogTable() {
        return catalogTable;
    }

    @VisibleForTesting
    @JsonIgnore
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @VisibleForTesting
    @JsonIgnore
    public ReadableConfig getReadableConfig() {
        return configuration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CatalogTableSpecBase that = (CatalogTableSpecBase) o;
        return objectIdentifier.equals(that.objectIdentifier)
                && catalogTable.toProperties().equals(that.catalogTable.toProperties());
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectIdentifier, catalogTable.toProperties());
    }
}
