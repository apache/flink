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

package org.apache.flink.fs.cse;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

/**
 * Factory for creating {@link KeyProvider} instances from Flink configuration.
 *
 * <p>Implementations must have a public no-arg constructor for reflective instantiation by
 * filesystem factories. The configuration passed to {@link #createKeyProvider} is the full
 * filesystem configuration. Implementations should read only CSE-scoped keys (prefixed with {@link
 * CseOptions#KEY_PREFIX}).
 */
@Internal
@Experimental
public interface KeyProviderFactory {

    /** Creates a key provider from the given CSE configuration. */
    KeyProvider createKeyProvider(ReadableConfig configuration) throws KeyProviderException;
}
