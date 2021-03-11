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

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;

/** Util class for the hive parser. */
public class HiveParserUtils {

    private HiveParserUtils() {}

    public static String generateErrorMessage(ASTNode ast, String message) {
        StringBuilder sb = new StringBuilder();
        if (ast == null) {
            sb.append(message).append(". Cannot tell the position of null AST.");
            return sb.toString();
        }
        sb.append(ast.getLine());
        sb.append(":");
        sb.append(ast.getCharPositionInLine());
        sb.append(" ");
        sb.append(message);
        sb.append(". Error encountered near token '");
        sb.append(ErrorMsg.getText(ast));
        sb.append("'");
        return sb.toString();
    }
}
