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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/** Utils for LogicalDistribution. */
public class LogicalDistributionUtils {

    private static final String LOGICAL_DISTRIBUTION_CLASS_NAME =
            "org.apache.flink.table.planner.plan.nodes.hive.LogicalDistribution";
    private static final Class logicalDistributionClz =
            HiveReflectionUtils.tryGetClass(LOGICAL_DISTRIBUTION_CLASS_NAME);

    public static RelNode create(RelNode input, RelCollation collation, List<Integer> distKeys) {
        try {
            return (RelNode)
                    HiveReflectionUtils.invokeMethod(
                            logicalDistributionClz,
                            null,
                            "create",
                            new Class[] {RelNode.class, RelCollation.class, List.class},
                            new Object[] {input, collation, distKeys});
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new FlinkHiveException("Failed to create LogicalDistribution", e);
        }
    }

    public static RelCollation getCollation(RelNode relNode) {
        try {
            return (RelCollation)
                    HiveReflectionUtils.invokeMethod(
                            logicalDistributionClz,
                            relNode,
                            "getCollation",
                            new Class[] {},
                            new Object[] {});
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new FlinkHiveException("Failed to getCollation", e);
        }
    }

    public static List<Integer> getDistKeys(RelNode relNode) {
        try {
            return (List<Integer>)
                    HiveReflectionUtils.invokeMethod(
                            logicalDistributionClz,
                            relNode,
                            "getDistKeys",
                            new Class[] {},
                            new Object[] {});
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new FlinkHiveException("Failed to getCollation", e);
        }
    }

    public static boolean isLogicalDistributionNode(RelNode relNode) {
        return relNode.getClass().getName().equals(LOGICAL_DISTRIBUTION_CLASS_NAME);
    }
}
