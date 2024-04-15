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

package org.apache.flink.table.catalog.listener;

import org.apache.flink.annotation.PublicEvolving;

/**
 * A listener that is notified on specific catalog changed in catalog manager.
 *
 * <p>It is highly recommended NOT to perform any blocking operation inside the callbacks. If you
 * block the thread the invoker of catalog manager is possibly blocked. You can perform the
 * operation asynchronously in an executor, but you need to handle timing issues.
 */
@PublicEvolving
public interface CatalogModificationListener {
    /** Callback on catalog modification such as database and table ddl operations. */
    void onEvent(CatalogModificationEvent event);
}
