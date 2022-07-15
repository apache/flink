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

package org.apache.flink.table.endpoint.hive.util;

import org.apache.flink.table.api.ValidationException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.hive.conf.SystemVariables.ENV_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.HIVECONF_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.HIVEVAR_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.METACONF_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.SYSTEM_PREFIX;

/**
 * Utils to normalize and validate hive jdbc conf.
 *
 * <p>Hive JDBC allows users to configure the session during the open session by setting values in
 * its jdbc URL. The syntax is
 *
 * <pre>
 *     jdbc:hive2://host1:port1,host2:port2/dbName;initFile=file;sess_var_list?hive_conf_list#hive_var_list
 * </pre>
 *
 * <p>For different parts in the URL, {@code HiveConnection} encodes different parts with different
 * prefix and store them into the configuration.
 */
public class HiveJdbcParameterUtils {

    private static final String SET_PREFIX = "set:";
    private static final String USE_PREFIX = "use:";
    private static final String USE_DATABASE = "database";

    public static Map<String, String> validateAndNormalize(Map<String, String> parameters) {
        Map<String, String> normalized = new HashMap<>();
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            String normalizedKey = key;
            if (key.startsWith(SET_PREFIX)) {
                String newKey = key.substring(SET_PREFIX.length());
                // TODO: use HiveParserSetProcessor when FLINK-28096 is fixed
                if (newKey.startsWith(ENV_PREFIX)) {
                    throw new ValidationException(
                            String.format(
                                    "Can not set env variables %s during the session connection.",
                                    key));
                } else if (newKey.startsWith(SYSTEM_PREFIX)) {
                    normalizedKey = newKey.substring(SYSTEM_PREFIX.length());
                } else if (newKey.startsWith(HIVECONF_PREFIX)) {
                    normalizedKey = newKey.substring(HIVECONF_PREFIX.length());
                } else if (newKey.startsWith(HIVEVAR_PREFIX)) {
                    normalizedKey = newKey.substring(HIVEVAR_PREFIX.length());
                } else if (newKey.startsWith(METACONF_PREFIX)) {
                    normalizedKey = newKey.substring(METACONF_PREFIX.length());
                } else {
                    normalizedKey = newKey;
                }
            } else if (key.startsWith(USE_PREFIX)) {
                // ignore use parameters
                continue;
            }
            normalized.put(normalizedKey, entry.getValue());
        }
        return normalized;
    }

    public static Optional<String> getUsedDefaultDatabase(Map<String, String> parameters) {
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(USE_PREFIX)) {
                // ignore
                continue;
            }
            if (!key.equals(USE_PREFIX + USE_DATABASE)) {
                throw new ValidationException(String.format("Unknown use parameter: %s.", key));
            }

            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }
}
