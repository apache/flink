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

/** Counterpart of hive's org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder. */
public class HiveParserASTBuilder {

    public static HiveParserASTBuilder construct(int tokenType, String text) {
        HiveParserASTBuilder b = new HiveParserASTBuilder();
        b.curr = createAST(tokenType, text);
        return b;
    }

    public static HiveParserASTNode createAST(int tokenType, String text) {
        return (HiveParserASTNode) HiveASTParseDriver.ADAPTOR.create(tokenType, text);
    }

    public static HiveParserASTNode qualifiedName(String tableName, String colName) {
        HiveParserASTBuilder b =
                HiveParserASTBuilder.construct(HiveASTParser.DOT, ".")
                        .add(
                                HiveParserASTBuilder.construct(
                                                HiveASTParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL")
                                        .add(HiveASTParser.Identifier, tableName))
                        .add(HiveASTParser.Identifier, colName);
        return b.node();
    }

    HiveParserASTNode curr;

    public HiveParserASTNode node() {
        return curr;
    }

    public HiveParserASTBuilder add(int tokenType, String text) {
        HiveASTParseDriver.ADAPTOR.addChild(curr, createAST(tokenType, text));
        return this;
    }

    public HiveParserASTBuilder add(HiveParserASTBuilder b) {
        HiveASTParseDriver.ADAPTOR.addChild(curr, b.curr);
        return this;
    }
}
