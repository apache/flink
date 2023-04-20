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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Optional;

/**
 * Describes a complete pipeline from one or more source tables to a sink table.
 *
 * @see Table#insertInto(String)
 */
@PublicEvolving
public interface TablePipeline extends Explainable<TablePipeline>, Executable, Compilable {

    /**
     * @return The sink table's {@link ObjectIdentifier}, if any. The result is empty for anonymous
     *     sink tables that haven't been registered before.
     */
    Optional<ObjectIdentifier> getSinkIdentifier();
}
