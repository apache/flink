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

package org.apache.flink.table.planner.delegation.hive.parse;

import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNode;
import org.apache.flink.table.planner.delegation.hive.copy.HiveParserASTNodeOrigin;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.ErrorMsg;

/** Util class to generate error messages with HiveParserASTNode. */
public class HiveParserErrorMsg {

    private static final String LINE_SEP = System.getProperty("line.separator");

    private HiveParserErrorMsg() {}

    public static String getMsg(ErrorMsg errorMsg, HiveParserASTNode tree, String reason) {
        return getMsg(errorMsg, tree) + ": " + reason;
    }

    public static String getMsg(ErrorMsg errorMsg, Tree tree, String reason) {
        return getMsg(errorMsg, (HiveParserASTNode) tree, reason);
    }

    public static String getMsg(ErrorMsg errorMsg, Tree tree) {
        return getMsg(errorMsg, (HiveParserASTNode) tree);
    }

    public static String getMsg(ErrorMsg errorMsg, HiveParserASTNode astNode) {
        StringBuilder sb = new StringBuilder();
        renderPosition(sb, astNode);
        sb.append(" ");
        sb.append(errorMsg.getMsg());
        sb.append(" '");
        sb.append(getText(astNode));
        sb.append("'");
        renderOrigin(sb, astNode.getOrigin());
        return sb.toString();
    }

    private static void renderPosition(StringBuilder sb, HiveParserASTNode tree) {
        sb.append("Line ");
        sb.append(getLine(tree));
        sb.append(":");
        sb.append(getCharPositionInLine(tree));
    }

    private static String getText(HiveParserASTNode tree) {
        if (tree.getChildCount() == 0) {
            return tree.getText();
        }
        return getText((HiveParserASTNode) tree.getChild(tree.getChildCount() - 1));
    }

    private static void renderOrigin(StringBuilder sb, HiveParserASTNodeOrigin origin) {
        while (origin != null) {
            sb.append(" in definition of ");
            sb.append(origin.getObjectType());
            sb.append(" ");
            sb.append(origin.getObjectName());
            sb.append(" [");
            sb.append(LINE_SEP);
            sb.append(origin.getObjectDefinition());
            sb.append(LINE_SEP);
            sb.append("] used as ");
            sb.append(origin.getUsageAlias());
            sb.append(" at ");
            HiveParserASTNode usageNode = origin.getUsageNode();
            renderPosition(sb, usageNode);
            origin = usageNode.getOrigin();
        }
    }

    private static int getLine(HiveParserASTNode tree) {
        if (tree.getChildCount() == 0) {
            return tree.getToken().getLine();
        }

        return getLine((HiveParserASTNode) tree.getChild(0));
    }

    private static int getCharPositionInLine(HiveParserASTNode tree) {
        if (tree.getChildCount() == 0) {
            return tree.getToken().getCharPositionInLine();
        }

        return getCharPositionInLine((HiveParserASTNode) tree.getChild(0));
    }
}
