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

package org.apache.flink.table.connector;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogLock;

/**
 * Source and sink implement this interface if they require {@link CatalogLock}.
 *
 * @deprecated This interface will be removed soon. Please see FLIP-346 for more details.
 */
@Deprecated
@Internal
public interface RequireCatalogLock {

    /** Set catalog lock factory to the connector. */
    void setLockFactory(CatalogLock.Factory lockFactory);
}
