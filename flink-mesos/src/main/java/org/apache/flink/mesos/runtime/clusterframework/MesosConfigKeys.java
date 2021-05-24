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

package org.apache.flink.mesos.runtime.clusterframework;

/**
 * The Mesos environment variables used for settings of the containers.
 *
 * @deprecated Apache Mesos support was deprecated in Flink 1.13 and is subject to removal in the
 *     future (see FLINK-22352 for further details).
 */
@Deprecated
public class MesosConfigKeys {
    // ------------------------------------------------------------------------
    //  Environment variable names
    // ------------------------------------------------------------------------

    /** Reserved for future enhancement. */
    public static final String ENV_FLINK_TMP_DIR = "_FLINK_TMP_DIR";

    /** JVM arguments, used by the JM and TM. */
    public static final String ENV_JVM_ARGS = "JVM_ARGS";

    /** Standard environment variables used in DCOS environment. */
    public static final String ENV_TASK_NAME = "TASK_NAME";

    /** Standard environment variables used in DCOS environment. */
    public static final String ENV_FRAMEWORK_NAME = "FRAMEWORK_NAME";

    /** Private constructor to prevent instantiation. */
    private MesosConfigKeys() {}
}
