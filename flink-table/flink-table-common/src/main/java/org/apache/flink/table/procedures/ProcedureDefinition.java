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

package org.apache.flink.table.procedures;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Definition of a procedure. We consider procedure as a kind of function, so make it implement
 * {@link FunctionDefinition}.
 */
@Internal
public class ProcedureDefinition implements FunctionDefinition {

    /** the name for the methods to be involved in the procedure. */
    public static final String PROCEDURE_CALL = "call";

    private final Procedure procedure;

    public ProcedureDefinition(Procedure procedure) {
        this.procedure = procedure;
    }

    @Override
    public FunctionKind getKind() {
        return FunctionKind.OTHER;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forProcedure(typeFactory, procedure.getClass());
    }
}
