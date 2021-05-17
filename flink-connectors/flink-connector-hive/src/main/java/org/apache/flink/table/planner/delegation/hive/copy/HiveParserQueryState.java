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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import java.util.Map;

/** Counterpart of hive's org.apache.hadoop.hive.ql.QueryState. */
public class HiveParserQueryState {
    /** current configuration. */
    private final HiveConf queryConf;
    /** type of the command. */
    private HiveOperation commandType;

    public HiveParserQueryState(HiveConf conf) {
        this(conf, null, false);
    }

    public HiveParserQueryState(HiveConf conf, Map<String, String> confOverlay, boolean runAsync) {
        this.queryConf = createConf(conf, confOverlay, runAsync);
    }

    /**
     * If there are query specific settings to overlay, then create a copy of config There are two
     * cases we need to clone the session config that's being passed to hive driver 1. Async query -
     * If the client changes a config setting, that shouldn't reflect in the execution already
     * underway 2. confOverlay - The query specific settings should only be applied to the query
     * config and not session
     *
     * @return new configuration
     */
    private HiveConf createConf(HiveConf conf, Map<String, String> confOverlay, boolean runAsync) {

        if (confOverlay != null && !confOverlay.isEmpty()) {
            conf = (conf == null ? new HiveConf() : new HiveConf(conf));

            // apply overlay query specific settings, if any
            for (Map.Entry<String, String> confEntry : confOverlay.entrySet()) {
                try {
                    conf.verifyAndSet(confEntry.getKey(), confEntry.getValue());
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Error applying statement specific settings", e);
                }
            }
        } else if (runAsync) {
            conf = (conf == null ? new HiveConf() : new HiveConf(conf));
        }

        if (conf == null) {
            conf = new HiveConf();
        }

        conf.setVar(HiveConf.ConfVars.HIVEQUERYID, QueryPlan.makeQueryId());
        return conf;
    }

    public String getCommandType() {
        if (commandType == null) {
            return null;
        }
        return commandType.getOperationName();
    }

    public void setCommandType(HiveOperation commandType) {
        this.commandType = commandType;
    }

    public HiveConf getConf() {
        return queryConf;
    }
}
