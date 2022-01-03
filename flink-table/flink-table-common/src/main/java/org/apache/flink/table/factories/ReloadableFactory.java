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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import java.util.Collections;
import java.util.Set;

/**
 * Extension of the {@link Factory} interface containing information required to reload the built
 * objects from a stored plan.
 *
 * @see <a href="https://cwiki.apache.org/confluence/x/KZBnCw">FLIP-190</a>
 */
@PublicEvolving
public interface ReloadableFactory extends Factory {

    /**
     * Returns a set of {@link ConfigOption} keys that can be safely mutated when redeploying a
     * query plan across Flink versions, e.g. options that don't mutate the query plan.
     *
     * <p>Each entry of the returned set must correspond to a key (and not a fallback key) of a
     * {@link ConfigOption} either in {@link #requiredOptions()} or in {@link #optionalOptions()}.
     */
    default Set<String> mutableOptionKeys() {
        return Collections.emptySet();
    }
}
