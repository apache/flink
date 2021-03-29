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

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.authorization.AuthorizationParseUtils. */
public class HiveParserAuthorizationParseUtils {

    private HiveParserAuthorizationParseUtils() {}

    public static PrincipalDesc getPrincipalDesc(HiveParserASTNode principal) {
        PrincipalType type = getPrincipalType(principal);
        if (type != null) {
            String text = principal.getChild(0).getText();
            String principalName = HiveParserBaseSemanticAnalyzer.unescapeIdentifier(text);
            return new PrincipalDesc(principalName, type);
        }
        return null;
    }

    private static PrincipalType getPrincipalType(HiveParserASTNode principal) {
        switch (principal.getType()) {
            case HiveASTParser.TOK_USER:
                return PrincipalType.USER;
            case HiveASTParser.TOK_GROUP:
                return PrincipalType.GROUP;
            case HiveASTParser.TOK_ROLE:
                return PrincipalType.ROLE;
            default:
                return null;
        }
    }
}
