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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.resource.ResourceManager;

/**
 * An {@link ExecutableOperation} represents an operation that is executed for its side effects.
 *
 * <p>This internal interface is proposed to reduce the maintenance of bloated {@link
 * TableEnvironmentImpl#executeInternal(Operation)} and improve code readability as execution logic
 * is co-located with the operation definition. Besides, this interface can be used to extend
 * user-defined operation with customized execution logic once this interface is stable and public
 * in the future.
 */
@Internal
public interface ExecutableOperation extends Operation {

    /**
     * Executes the given operation and return the execution result.
     *
     * @param ctx the context to execute the operation.
     * @return the content of the execution result.
     * @see org.apache.flink.table.api.internal.TableEnvironmentInternal#executeInternal(Operation)
     */
    TableResultInternal execute(Context ctx);

    /**
     * The context to execute the operation. Operation may make side effect to the context, e.g.
     * catalog manager, configuration.
     */
    @Internal
    interface Context {

        CatalogManager getCatalogManager();

        FunctionCatalog getFunctionCatalog();

        ModuleManager getModuleManager();

        ResourceManager getResourceManager();

        TableConfig getTableConfig();

        boolean isStreamingMode();
    }
}
