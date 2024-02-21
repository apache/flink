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

import java.io.Closeable;
import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * An interface that allows source and sink to use global lock to some transaction-related things.
 *
 * @deprecated This interface will be removed soon. Please see FLIP-346 for more details.
 */
@Deprecated
@Internal
public interface CatalogLock extends Closeable {

    /** Run with catalog lock. The caller should tell catalog the database and table name. */
    <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception;

    /**
     * Factory to create {@link CatalogLock}.
     *
     * @deprecated This interface will be removed soon. Please see FLIP-346 for more details.
     */
    @Deprecated
    @Internal
    interface Factory extends Serializable {
        CatalogLock create();
    }
}
