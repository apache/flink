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

package org.apache.flink.table.runtime.operators.sink.constraint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Interface for constraints to be enforced on the input {@link RowData}. */
@Internal
public interface Constraint extends Serializable {

    /**
     * If a null value is returned, the input row is considered to be invalid and should be dropped.
     * This applies to all nested columns. If at any level a null value is returned, the incoming
     * row is dropped from further processing.
     */
    @Nullable
    RowData enforce(RowData input);
}
