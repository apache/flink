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

package org.apache.flink.runtime.security.token;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Container for delegation tokens. */
@Experimental
public class DelegationTokenContainer implements Serializable {
    private Map<String, byte[]> tokens = new HashMap<>();

    public Map<String, byte[]> getTokens() {
        return tokens;
    }

    public void addToken(String key, byte[] value) {
        tokens.put(key, value);
    }

    public boolean hasTokens() {
        return !tokens.isEmpty();
    }
}
