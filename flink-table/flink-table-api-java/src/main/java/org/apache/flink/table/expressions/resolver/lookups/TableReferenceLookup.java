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

package org.apache.flink.table.expressions.resolver.lookups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.TableReferenceExpression;

import java.util.Optional;

/** Provides a way to look up table reference by the name of the table. */
@Internal
public interface TableReferenceLookup {

    /**
     * Tries to resolve given name to {@link TableReferenceExpression}.
     *
     * @param name name of table to look for
     * @return resolved field reference or empty if could not find table with given name.
     */
    Optional<TableReferenceExpression> lookupTable(String name);
}
