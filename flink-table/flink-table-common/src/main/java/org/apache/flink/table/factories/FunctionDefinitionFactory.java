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

package org.apache.flink.table.factories;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.functions.FunctionDefinition;

/** A factory to create {@link FunctionDefinition}. */
public interface FunctionDefinitionFactory {

    /**
     * Creates a {@link FunctionDefinition} from given {@link CatalogFunction}.
     *
     * @param name name of the {@link CatalogFunction}
     * @param catalogFunction the catalog function
     * @return a {@link FunctionDefinition}
     */
    FunctionDefinition createFunctionDefinition(String name, CatalogFunction catalogFunction);
}
