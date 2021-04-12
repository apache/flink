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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Arrays;
import java.util.List;

/**
 * Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.translator.JoinTypeCheckCtx.
 */
public class HiveParserJoinTypeCheckCtx extends HiveParserTypeCheckCtx {

    private final List<HiveParserRowResolver> inputRRLst;

    public HiveParserJoinTypeCheckCtx(
            HiveParserRowResolver leftRR,
            HiveParserRowResolver rightRR,
            JoinType hiveJoinType,
            FrameworkConfig frameworkConfig,
            RelOptCluster cluster)
            throws SemanticException {
        super(
                HiveParserRowResolver.getCombinedRR(leftRR, rightRR),
                true,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                false,
                frameworkConfig,
                cluster);
        this.inputRRLst = Arrays.asList(leftRR, rightRR);
    }

    /** @return the inputRR List */
    public List<HiveParserRowResolver> getInputRRList() {
        return inputRRLst;
    }
}
