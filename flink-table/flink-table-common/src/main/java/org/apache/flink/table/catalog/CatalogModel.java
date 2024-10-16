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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.Schema;

import javax.annotation.Nullable;

import java.util.Map;

/** Interface for a model in a catalog. */
@PublicEvolving
public interface CatalogModel {
    /** Returns a map of string-based model options. */
    Map<String, String> getOptions();

    /**
     * Get the unresolved input schema of the model.
     *
     * @return unresolved input schema of the model.
     */
    Schema getInputSchema();

    /**
     * Get the unresolved output schema of the model.
     *
     * @return unresolved output schema of the model.
     */
    Schema getOutputSchema();

    /**
     * Get comment of the model.
     *
     * @return comment of the model.
     */
    String getComment();

    /**
     * Get a deep copy of the CatalogModel instance.
     *
     * @return a copy of the CatalogModel instance
     */
    CatalogModel copy();

    /**
     * Copy the input model options into the CatalogModel instance.
     *
     * @return a copy of the CatalogModel instance with new model options.
     */
    CatalogModel copy(Map<String, String> options);

    /**
     * Creates a basic implementation of this interface.
     *
     * @param inputSchema unresolved input schema
     * @param outputSchema unresolved output schema
     * @param modelOptions model options
     * @param comment optional comment
     */
    static CatalogModel of(
            Schema inputSchema,
            Schema outputSchema,
            Map<String, String> modelOptions,
            @Nullable String comment) {
        return new DefaultCatalogModel(inputSchema, outputSchema, modelOptions, comment);
    }
}
