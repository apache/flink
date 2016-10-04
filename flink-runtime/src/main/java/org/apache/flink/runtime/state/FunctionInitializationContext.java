/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface provides a context in which user functions can initialize by registering to managed state (i.e. state
 * that is managed by state backends).
 *
 * <p>
 * Operator state is available to all functions, while keyed state is only available for functions after keyBy.
 *
 * <p>
 * For the purpose of initialization, the context signals if the state is empty or was restored from a previous
 * execution.
 *
 */
@PublicEvolving
public interface FunctionInitializationContext extends ManagedInitializationContext {
}
