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
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import java.io.Serializable;

/** Keep in sync with {@link ExecutionConfigOptions.TypeLengthEnforcer}. */
@Internal
enum TypeLengthEnforcementStrategy implements Serializable {
    TRIM_PAD,
    THROW;

    public static TypeLengthEnforcementStrategy of(
            ExecutionConfigOptions.TypeLengthEnforcer option) {
        switch (option) {
            case TRIM_PAD:
                return TRIM_PAD;
            case ERROR:
                return THROW;
            case IGNORE:
            // We should not create a constraint for this case.
            default:
                throw new IllegalArgumentException("Unknown type length enforcer: " + option);
        }
    }
}
