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

package org.apache.flink.connectors.hive.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

/** Utils to create HiveConf, see FLINK-20913 for more information. */
public class HiveConfUtils {

    /**
     * Create HiveConf instance via Hadoop configuration. Since {@link
     * HiveConf#HiveConf(org.apache.hadoop.conf.Configuration, java.lang.Class)} will override
     * properties in Hadoop configuration with Hive default values ({@link org.apache
     * .hadoop.hive.conf.HiveConf.ConfVars}), so we should use this method to create HiveConf
     * instance via Hadoop configuration.
     *
     * @param conf Hadoop configuration
     * @return HiveConf instance
     */
    public static HiveConf create(Configuration conf) {
        HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
        // to make sure Hive configuration properties in conf not be overridden
        hiveConf.addResource(conf);
        return hiveConf;
    }
}
