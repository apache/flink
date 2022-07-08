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

package org.apache.flink.table.planner.factories;

import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;

/**
 * Use TestFunctionDefinitionFactory to test loading function to ensure the function can be loaded
 * correctly if only implement legacy interface {@link
 * FunctionDefinitionFactory#createFunctionDefinition(String, CatalogFunction)}.
 */
public class TestFunctionDefinitionFactory implements FunctionDefinitionFactory {

    public FunctionDefinition createFunctionDefinition(
            String name, CatalogFunction catalogFunction) {
        return UserDefinedFunctionHelper.instantiateFunction(
                Thread.currentThread().getContextClassLoader(), null, name, catalogFunction);
    }
}
