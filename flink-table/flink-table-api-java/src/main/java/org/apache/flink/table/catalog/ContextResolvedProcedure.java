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
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.procedures.Procedure;

/**
 * This class contains information about a procedure and its relationship with a {@link Catalog}.
 */
@Internal
public class ContextResolvedProcedure {

    // we reuse FunctionIdentifier as the identifier for procedure,
    // since procedure and function are almost same
    private final FunctionIdentifier procedureIdentifier;

    private final Procedure procedure;

    public ContextResolvedProcedure(FunctionIdentifier procedureIdentifier, Procedure procedure) {
        this.procedureIdentifier = procedureIdentifier;
        this.procedure = procedure;
    }

    public FunctionIdentifier getIdentifier() {
        return procedureIdentifier;
    }

    public Procedure getProcedure() {
        return procedure;
    }
}
