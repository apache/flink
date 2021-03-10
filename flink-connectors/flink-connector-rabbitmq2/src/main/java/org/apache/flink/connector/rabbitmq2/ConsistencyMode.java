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

package org.apache.flink.connector.rabbitmq2;

/**
 * The different consistency modes that can be defined for the sink and source individually.
 *
 * <p>The available consistency modes are as follows.
 *
 * <ul>
 *   <li><code>AT_MOST_ONCE</code> Messages are consumed by the output once or never.
 *   <li><code>AT_LEAST_ONCE</code> Messages are consumed by the output at least once.
 *   <li><code>EXACTLY_ONCE</code> Messages are consumed by the output exactly once.
 * </ul>
 *
 * <p>Note that the higher the consistency guarantee gets, fewer messages can be processed by the
 * system. At-least-once and exactly-once should only be used if necessary.
 */
public enum ConsistencyMode {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE,
}
