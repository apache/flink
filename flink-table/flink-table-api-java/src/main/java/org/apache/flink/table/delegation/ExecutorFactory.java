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

package org.apache.flink.table.delegation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.factories.Factory;

/**
 * Factory that creates an {@link Executor} for submitting table programs.
 *
 * <p>The factory is used with Java's Service Provider Interfaces (SPI) for discovering. See {@link
 * Factory} for more information.
 *
 * <p>Usually, there should only be one executor factory in the class path. However, advanced users
 * can implement a custom one for hooking into the submission process.
 *
 * <p><b>Important:</b> Implementations of this interface should also implement the method
 *
 * <pre>
 *   public Executor create(Configuration, StreamExecutionEnvironment);
 * </pre>
 *
 * <p>This method will be used when instantiating a {@link TableEnvironment} from one of the
 * bridging modules which enables conversion from/to {@code DataStream} API.
 */
@Internal
public interface ExecutorFactory extends Factory {

    String DEFAULT_IDENTIFIER = "default";

    /** Creates a corresponding {@link Executor}. */
    Executor create(Configuration configuration);
}
