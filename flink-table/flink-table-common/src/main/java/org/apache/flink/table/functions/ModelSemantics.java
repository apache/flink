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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;

/**
 * Provides call information about the model that has been passed to a model argument.
 *
 * <p>This class is only available for model arguments (i.e. arguments of a {@link
 * ProcessTableFunction} that are annotated with {@code @ArgumentHint(MODEL)}).
 */
@PublicEvolving
public interface ModelSemantics {

    /** Input data type expected by the passed model. */
    DataType inputDataType();

    /** Output data type produced by the passed model. */
    DataType outputDataType();
}
