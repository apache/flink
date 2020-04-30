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

/**
 * This package holds the classes of the <b>internal state type hierarchy</b>.
 *
 * <p>The internal state classes give access to the namespace getters and setters and access to
 * additional functionality, like raw value access or state merging.
 *
 * <p>The public API state hierarchy is intended to be programmed against by Flink applications.
 * The internal state hierarchy holds all the auxiliary methods that are used by the runtime and not
 * intended to be used by user applications. These internal methods are considered of limited use to users and
 * only confusing, and are usually not regarded as stable across releases.
 *
 * <p>Each specific type in the internal state hierarchy extends the type from the public
 * state hierarchy. The following illustrates the relationship between the public- and the internal
 * hierarchy at the example of a subset of the classes:
 *
 * <pre>
 *             State
 *               |
 *               +-------------------InternalKvState
 *               |                         |
 *          MergingState                   |
 *               |                         |
 *               +-----------------InternalMergingState
 *               |                         |
 *      +--------+------+                  |
 *      |               |                  |
 * ReducingState    ListState        +-----+-----------------+
 *      |               |            |                       |
 *      |               +-----------   -----------------InternalListState
 *      |                            |
 *      +------------------InternalReducingState
 * </pre>
 */
package org.apache.flink.runtime.state.internal;