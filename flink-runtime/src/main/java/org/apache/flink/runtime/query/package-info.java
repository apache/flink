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
 * This package contains all KvState query related classes.
 *
 * <h2>TaskManager and JobManager</h2>
 *
 * <p>State backends register queryable state instances at the {@link
 * org.apache.flink.runtime.query.KvStateRegistry}.
 * There is one registry per TaskManager. Registered KvState instances are
 * reported to the JobManager, where they are aggregated at the {@link
 * org.apache.flink.runtime.query.KvStateLocationRegistry}.
 *
 * <p>Instances of {@link org.apache.flink.runtime.query.KvStateLocation} contain
 * all information needed for a client to query a KvState instance.
 *
 * <p>See also:
 * <ul>
 * <li>{@link org.apache.flink.runtime.query.KvStateRegistry}</li>
 * <li>{@link org.apache.flink.runtime.query.TaskKvStateRegistry}</li>
 * <li>{@link org.apache.flink.runtime.query.KvStateLocation}</li>
 * <li>{@link org.apache.flink.runtime.query.KvStateLocationRegistry}</li>
 * </ul>
 *
 * <h2>Client</h2>
 *
 * The {@link org.apache.flink.runtime.query.QueryableStateClient} is used
 * to query KvState instances. The client takes care of {@link
 * org.apache.flink.runtime.query.KvStateLocation} lookup and caching. Queries
 * are then dispatched via the network client.
 *
 * <h3>JobManager Communication</h3>
 *
 * <p>The JobManager is queried for {@link org.apache.flink.runtime.query.KvStateLocation}
 * instances via the {@link org.apache.flink.runtime.query.KvStateLocationLookupService}.
 * The client caches resolved locations and dispatches queries directly to the
 * TaskManager.
 *
 * <h3>TaskManager Communication</h3>
 *
 * <p>After the location has been resolved, the TaskManager is queried via the
 * {@link org.apache.flink.runtime.query.netty.KvStateClient}.
 */
package org.apache.flink.runtime.query;
