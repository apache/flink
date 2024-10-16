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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A catalog model implementation. */
@Internal
public class DefaultCatalogModel implements CatalogModel {
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final Map<String, String> modelOptions;
    private final @Nullable String comment;

    protected DefaultCatalogModel(
            Schema inputSchema,
            Schema outputSchema,
            Map<String, String> modelOptions,
            @Nullable String comment) {
        this.inputSchema = checkNotNull(inputSchema, "Input schema must not be null.");
        this.outputSchema = checkNotNull(outputSchema, "Output schema must not be null.");
        this.modelOptions = modelOptions;
        this.comment = comment;
    }

    @Override
    public Map<String, String> getOptions() {
        return modelOptions;
    }

    @Override
    public Schema getInputSchema() {
        return inputSchema;
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }

    @Override
    public String getComment() {
        return comment;
    }

    @Override
    public CatalogModel copy() {
        return new DefaultCatalogModel(
                this.inputSchema, this.outputSchema, this.modelOptions, this.comment);
    }

    @Override
    public CatalogModel copy(Map<String, String> newModelOptions) {
        return new DefaultCatalogModel(
                this.inputSchema, this.outputSchema, newModelOptions, this.comment);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCatalogModel that = (DefaultCatalogModel) o;
        return Objects.equals(inputSchema, that.inputSchema)
                && Objects.equals(outputSchema, that.outputSchema)
                && modelOptions.equals(that.modelOptions)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputSchema, outputSchema, modelOptions, comment);
    }

    @Override
    public String toString() {
        return "DefaultCatalogModel{"
                + "inputSchema="
                + inputSchema
                + ", outputSchema="
                + outputSchema
                + ", modelOptions="
                + modelOptions
                + ", comment="
                + comment
                + "}";
    }
}
