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

package org.apache.flink.configuration;

import org.apache.flink.annotation.Public;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This class contains all constants for the configuration. That includes the configuration keys and
 * the default values.
 */
@Public
@SuppressWarnings("unused")
public final class ConfigConstants {

    // ------------------------------------------------------------------------
    //                            Configuration Keys
    // ------------------------------------------------------------------------

    // ---------------------------- Metrics -----------------------------------

    /**
     * The prefix for per-metric reporter configs. Has to be combined with a reporter name and the
     * configs mentioned below.
     */
    public static final String METRICS_REPORTER_PREFIX = "metrics.reporter.";
    /**
     * The prefix for per-trace reporter configs. Has to be combined with a reporter name and the
     * configs mentioned below.
     */
    public static final String TRACES_REPORTER_PREFIX = "traces.reporter.";

    // ------------------------------------------------------------------------
    //                            Default Values
    // ------------------------------------------------------------------------

    // ------ Common Resource Framework Configuration (YARN & Mesos) ------

    /** Start command template for Flink on YARN containers. */
    public static final String DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE =
            "%java% %jvmmem% %jvmopts% %logging% %class% %args% %redirects%";

    // ----------------------------- LocalExecution ----------------------------

    public static final int DEFAULT_LOCAL_NUMBER_TASK_MANAGER = 1;

    // ----------------------------- Environment Variables ----------------------------

    /** The environment variable name which contains the location of the configuration directory. */
    public static final String ENV_FLINK_CONF_DIR = "FLINK_CONF_DIR";

    /** The environment variable name which contains the location of the lib folder. */
    public static final String ENV_FLINK_LIB_DIR = "FLINK_LIB_DIR";

    /**
     * The default Flink lib directory if none has been specified via {@link #ENV_FLINK_LIB_DIR}.
     */
    public static final String DEFAULT_FLINK_LIB_DIR = "lib";

    /** The environment variable name which contains the location of the opt directory. */
    public static final String ENV_FLINK_OPT_DIR = "FLINK_OPT_DIR";

    /**
     * The default Flink opt directory if none has been specified via {@link #ENV_FLINK_OPT_DIR}.
     */
    public static final String DEFAULT_FLINK_OPT_DIR = "opt";

    /** The environment variable name which contains the location of the plugins folder. */
    public static final String ENV_FLINK_PLUGINS_DIR = "FLINK_PLUGINS_DIR";

    /**
     * The default Flink plugins directory if none has been specified via {@link
     * #ENV_FLINK_PLUGINS_DIR}.
     */
    public static final String DEFAULT_FLINK_PLUGINS_DIRS = "plugins";

    /** The environment variable name which contains the Flink installation root directory. */
    public static final String ENV_FLINK_HOME_DIR = "FLINK_HOME";

    /** The user lib directory name. */
    public static final String DEFAULT_FLINK_USR_LIB_DIR = "usrlib";

    /**
     * The environment variable name which contains a list of newline-separated HTTP headers for
     * Flink's REST client.
     */
    public static final String FLINK_REST_CLIENT_HEADERS = "FLINK_REST_CLIENT_HEADERS";

    // ---------------------------- Encoding ------------------------------

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /** Not instantiable. */
    private ConfigConstants() {}
}
