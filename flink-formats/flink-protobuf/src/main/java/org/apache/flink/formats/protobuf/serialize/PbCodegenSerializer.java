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

package org.apache.flink.formats.protobuf.serialize;

import org.apache.flink.formats.protobuf.PbCodegenException;

/**
 * {@link PbCodegenSerializer} is responsible for converting flink internal data object to protobuf
 * object by codegen process. The codegen procedure could be considered as
 *
 * <PRE>{@code resultVariableCode = codegen(flinkObjectCode) }
 * </PRE>
 */
public interface PbCodegenSerializer {
    /**
     * @param resultVar the final var name that is calculated by codegen. This var name will be used
     *     by outsider codegen environment. {@code resultVariable} should be protobuf object
     * @param flinkObjectCode may be a variable or expression. Current codegen environment can use
     *     this literal name directly to access the input. {@code flinkObject} should be a flink
     *     internal object.
     * @return The java code generated
     */
    String codegen(String resultVar, String flinkObjectCode, int indent) throws PbCodegenException;
}
