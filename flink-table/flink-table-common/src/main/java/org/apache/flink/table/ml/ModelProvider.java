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

package org.apache.flink.table.ml;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedCatalogModel;

/**
 * Model Provider defines how to handle models from a particular provider. For example, how to do
 * model inference for the models from a provider.
 *
 * <p>There could be but not limited to these types of model providers:
 *
 * <ul>
 *   <li>Public vendors. (e.g.OpenAI, Anthropic, Deepseek etc.)
 *   <li>Flink native. (model trained natively by Flink)
 * </ul>
 */
@PublicEvolving
public interface ModelProvider {

    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    ModelProvider copy();

    /** Context for creating runtime providers. */
    @PublicEvolving
    interface Context {

        /** Resolved catalog model. */
        ResolvedCatalogModel getCatalogModel();

        /**
         * Runtime config provided to provider. The config can be used by planner or model provider
         * at runtime. For example, async options can be used by planner to choose async inference.
         * Other config such as http timeout or retry can be used to configure model provider
         * runtime http client when calling external model providers such as OpenAI.
         */
        ReadableConfig runtimeConfig();
    }
}
