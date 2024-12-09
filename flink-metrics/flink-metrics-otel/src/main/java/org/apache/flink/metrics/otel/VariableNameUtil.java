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

package org.apache.flink.metrics.otel;

/**
 * Util for dealing with variables names in OTel reporting. Used to remove angel brackets that
 * metric group codes adds to variables.
 */
public class VariableNameUtil {
    private VariableNameUtil() {}

    /** Removes leading and trailing angle brackets. See ScopeFormat::SCOPE_VARIABLE_PREFIX. */
    public static String getVariableName(String str) {
        if (str.length() >= 2 && str.charAt(0) == '<' && str.charAt(str.length() - 1) == '>') {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }
}
