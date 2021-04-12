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

package org.apache.flink.api.common.externalresource;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;
import java.util.Optional;

/** Contains the information of an external resource. */
@PublicEvolving
public interface ExternalResourceInfo {

    /**
     * Get the property indicated by the specified key.
     *
     * @param key of the required property
     * @return an {@code Optional} containing the value associated to the key, or an empty {@code
     *     Optional} if no value has been stored under the given key
     */
    Optional<String> getProperty(String key);

    /**
     * Get all property keys.
     *
     * @return collection of all property keys
     */
    Collection<String> getKeys();
}
