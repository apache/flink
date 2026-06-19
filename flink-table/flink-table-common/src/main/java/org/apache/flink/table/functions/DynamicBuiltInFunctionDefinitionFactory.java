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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Experimental;

import java.util.List;

/**
 * {@link BuiltInFunctionDefinition} factory for dynamic function registration. This could be useful
 * when functions wanted to be automatically loaded from SQL modules. A good example usage is that
 * state processor API SQL module is providing extra table functions from Kafka connector.
 */
@Experimental
public interface DynamicBuiltInFunctionDefinitionFactory {
    /**
     * Returns the unique identifier of the factory. The suggested pattern is the following:
     * [module-name].[factory-name]. Such case modules can load all [module-name] prefixed functions
     * which belong to them.
     */
    String factoryIdentifier();

    /** Returns list of {@link BuiltInFunctionDefinition} which can be registered dynamically. */
    List<BuiltInFunctionDefinition> getBuiltInFunctionDefinitions();
}
