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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.util.CollectionUtil;

/** A factory to create {@link FunctionDefinition}. */
@PublicEvolving
public interface FunctionDefinitionFactory {

    /**
     * Creates a {@link FunctionDefinition} from given {@link CatalogFunction}.
     *
     * @param name name of the {@link CatalogFunction}
     * @param catalogFunction the catalog function
     * @return a {@link FunctionDefinition}
     */
    FunctionDefinition createFunctionDefinition(String name, CatalogFunction catalogFunction);

    /**
     * Creates a {@link FunctionDefinition} from given {@link CatalogFunction}. If the {@link
     * CatalogFunction} is created by user defined resource, the user of {@link
     * FunctionDefinitionFactory} needs to override this method explicitly. The implementation logic
     * needs to use the user classloader to load custom classes instead of the thread context
     * classloader.
     *
     * @param name name of the {@link CatalogFunction}
     * @param catalogFunction the catalog function
     * @param userClassLoader the class loader is used to load user defined function's class
     * @return a {@link FunctionDefinition}
     */
    default FunctionDefinition createFunctionDefinition(
            String name, CatalogFunction catalogFunction, ClassLoader userClassLoader) {
        if (!CollectionUtil.isNullOrEmpty(catalogFunction.getFunctionResources())) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%s need to override default createFunctionDefinition for "
                                    + "loading user defined function class",
                            this.getClass().getSimpleName()));
        } else {
            return createFunctionDefinition(name, catalogFunction);
        }
    }
}
