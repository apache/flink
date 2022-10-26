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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.api.TableConfig;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.hadoop.hive.conf.SystemVariables.ENV_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.HIVECONF_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.HIVEVAR_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.METACONF_PREFIX;
import static org.apache.hadoop.hive.conf.SystemVariables.SYSTEM_PREFIX;

/** Counterpart of hive's {@link org.apache.hadoop.hive.ql.processors.SetProcessor}. */
public class HiveSetProcessor {

    private static final String[] PASSWORD_STRINGS = new String[] {"password", "paswd", "pswd"};

    /** Set variable following Hive's implementation. */
    public static void setVariable(
            HiveConf hiveConf, Map<String, String> hiveVariables, String varname, String varvalue) {
        if (varname.startsWith(ENV_PREFIX)) {
            throw new UnsupportedOperationException("env:* variables can not be set.");
        } else if (varname.startsWith(SYSTEM_PREFIX)) {
            String propName = varname.substring(SYSTEM_PREFIX.length());
            System.getProperties()
                    .setProperty(
                            propName,
                            new VariableSubstitution(() -> hiveVariables)
                                    .substitute(hiveConf, varvalue));
        } else if (varname.startsWith(HIVECONF_PREFIX)) {
            String propName = varname.substring(HIVECONF_PREFIX.length());
            setConf(hiveConf, hiveVariables, varname, propName, varvalue);
        } else if (varname.startsWith(HIVEVAR_PREFIX)) {
            String propName = varname.substring(HIVEVAR_PREFIX.length());
            hiveVariables.put(
                    propName,
                    new VariableSubstitution(() -> hiveVariables).substitute(hiveConf, varvalue));
        } else if (varname.startsWith(METACONF_PREFIX)) {
            String propName = varname.substring(METACONF_PREFIX.length());
            try {
                Hive hive = Hive.get(hiveConf);
                hive.setMetaConf(
                        propName,
                        new VariableSubstitution(() -> hiveVariables)
                                .substitute(hiveConf, varvalue));
            } catch (HiveException e) {
                throw new FlinkHiveException(
                        String.format("'SET %s=%s' FAILED.", varname, varvalue), e);
            }
        } else {
            // here is a little of different from Hive's behavior,
            // if there's no prefix, we also put it to passed hiveVariables for flink
            // may use it as its own configurations.
            // Otherwise, there's no way to set Flink's configuration using Hive's set command.
            hiveVariables.put(
                    varname,
                    new VariableSubstitution(() -> hiveVariables).substitute(hiveConf, varvalue));
            setConf(hiveConf, hiveVariables, varname, varname, varvalue);
        }
    }

    /**
     * check whether the variable's name is started with the special variable prefix that Hive
     * reserves.
     */
    public static boolean startWithHiveSpecialVariablePrefix(String varname) {
        String[] hiveSpecialVariablePrefix =
                new String[] {
                    ENV_PREFIX, SYSTEM_PREFIX, HIVECONF_PREFIX, HIVEVAR_PREFIX, METACONF_PREFIX
                };
        for (String prefix : hiveSpecialVariablePrefix) {
            if (varname.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static void setConf(
            HiveConf hiveConf,
            Map<String, String> hiveVariables,
            String varname,
            String key,
            String varvalue) {
        String value = new VariableSubstitution(() -> hiveVariables).substitute(hiveConf, varvalue);
        if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVECONFVALIDATION)) {
            HiveConf.ConfVars confVars = HiveConf.getConfVars(key);
            if (confVars != null) {
                if (!confVars.isType(value)) {
                    String message =
                            String.format(
                                    "'SET %s=%s' FAILED because %s expects %s type value.",
                                    varname, varvalue, key, confVars.typeString());
                    throw new IllegalArgumentException(message);
                }
                String fail = confVars.validate(value);
                if (fail != null) {
                    String message =
                            String.format(
                                    "'SET %s=%s' FAILED in validation : %s.",
                                    varname, varvalue, fail);
                    throw new IllegalArgumentException(message);
                }
            }
        }
        hiveConf.verifyAndSet(key, value);
    }

    public static String getVariable(
            Map<String, String> flinkConf,
            HiveConf hiveConf,
            Map<String, String> hiveVariables,
            String varname) {
        if (varname.equals("silent")) {
            return "silent is not a valid variable";
        }
        if (varname.startsWith(SYSTEM_PREFIX)) {
            String propName = varname.substring(SYSTEM_PREFIX.length());
            String result = System.getProperty(propName);
            if (result != null) {
                if (isHidden(propName)) {
                    return SYSTEM_PREFIX + propName + " is a hidden config";
                } else {
                    return SYSTEM_PREFIX + propName + "=" + result;
                }
            } else {
                return propName + " is undefined as a system property";
            }
        } else if (varname.indexOf(ENV_PREFIX) == 0) {
            String var = varname.substring(ENV_PREFIX.length());
            if (System.getenv(var) != null) {
                if (isHidden(var)) {
                    return ENV_PREFIX + var + " is a hidden config";
                } else {
                    return ENV_PREFIX + var + "=" + System.getenv(var);
                }
            } else {
                return varname + " is undefined as an environmental variable";
            }
        } else if (varname.indexOf(HIVECONF_PREFIX) == 0) {
            String var = varname.substring(HIVECONF_PREFIX.length());
            if (hiveConf.isHiddenConfig(var)) {
                return HIVECONF_PREFIX + var + " is a hidden config";
            }
            if (hiveConf.get(var) != null) {
                return HIVECONF_PREFIX + var + "=" + hiveConf.get(var);
            } else {
                return varname + " is undefined as a hive configuration variable";
            }
        } else if (varname.indexOf(HIVEVAR_PREFIX) == 0) {
            String var = varname.substring(HIVEVAR_PREFIX.length());
            if (hiveVariables.get(var) != null) {
                return HIVEVAR_PREFIX + var + "=" + hiveVariables.get(var);
            } else {
                return varname + " is undefined as a hive variable";
            }
        } else if (varname.indexOf(METACONF_PREFIX) == 0) {
            String var = varname.substring(METACONF_PREFIX.length());
            String value;
            try {
                Hive hive = Hive.get(hiveConf);
                value = hive.getMetaConf(var);
            } catch (HiveException e) {
                throw new FlinkHiveException(
                        String.format("Failed to get variable for %s.", varname), e);
            }

            if (value != null) {
                return METACONF_PREFIX + var + "=" + value;
            } else {
                return varname + " is undefined as a hive meta variable";
            }
        } else {
            return dumpOption(flinkConf, hiveConf, hiveVariables, varname);
        }
    }

    /*
     * Checks if the value contains any of the PASSWORD_STRINGS and if yes
     * return true
     */
    private static boolean isHidden(String key) {
        for (String p : PASSWORD_STRINGS) {
            if (key.toLowerCase().contains(p)) {
                return true;
            }
        }
        return false;
    }

    private static String dumpOption(
            Map<String, String> flinkConf,
            HiveConf hiveConf,
            Map<String, String> hiveVariables,
            String s) {
        if (flinkConf.get(s) != null) {
            return s + "=" + flinkConf.get(s);
        } else if (hiveConf.isHiddenConfig(s)) {
            return s + " is a hidden config";
        } else if (hiveConf.get(s) != null) {
            return s + "=" + hiveConf.get(s);
        } else if (hiveVariables.containsKey(s)) {
            return s + "=" + hiveVariables.get(s);
        } else {
            return s + " is undefined";
        }
    }

    public static List<String> dumpOptions(
            Properties p,
            HiveConf hiveConf,
            Map<String, String> hiveVariables,
            TableConfig tableConfig) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        List<String> optionsList = new ArrayList<>();
        for (Object one : p.keySet()) {
            String oneProp = (String) one;
            String oneValue = p.getProperty(oneProp);
            if (hiveConf.isHiddenConfig(oneProp)) {
                continue;
            }
            sortedMap.put(HIVECONF_PREFIX + oneProp, oneValue);
        }

        // Inserting hive variables
        for (String s : hiveVariables.keySet()) {
            sortedMap.put(HIVEVAR_PREFIX + s, hiveVariables.get(s));
        }

        for (Map.Entry<String, String> entries : sortedMap.entrySet()) {
            optionsList.add(entries.getKey() + "=" + entries.getValue());
        }

        for (Map.Entry<String, String> entry : mapToSortedMap(System.getenv()).entrySet()) {
            if (isHidden(entry.getKey())) {
                continue;
            }
            optionsList.add(ENV_PREFIX + entry.getKey() + "=" + entry.getValue());
        }

        for (Map.Entry<String, String> entry :
                propertiesToSortedMap(System.getProperties()).entrySet()) {
            if (isHidden(entry.getKey())) {
                continue;
            }
            optionsList.add(SYSTEM_PREFIX + entry.getKey() + "=" + entry.getValue());
        }

        // Insert Flink table config variable
        for (Map.Entry<String, String> entry :
                mapToSortedMap(tableConfig.getConfiguration().toMap()).entrySet()) {
            optionsList.add(entry.getKey() + "=" + entry.getValue());
        }

        return optionsList;
    }

    private static SortedMap<String, String> mapToSortedMap(Map<String, String> data) {
        return new TreeMap<>(data);
    }

    private static SortedMap<String, String> propertiesToSortedMap(Properties p) {
        SortedMap<String, String> sortedPropMap = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : p.entrySet()) {
            sortedPropMap.put((String) entry.getKey(), (String) entry.getValue());
        }
        return sortedPropMap;
    }
}
