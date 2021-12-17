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

import org.antlr.runtime.TokenRewriteStream;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.SubQueryDiagnostic. */
public class HiveParserSubQueryDiagnostic {

    static QBSubQueryRewrite getRewrite(
            HiveParserQBSubQuery subQuery, TokenRewriteStream stream, HiveParserContext ctx) {
        if (ctx.isExplainSkipExecution()) {
            return new QBSubQueryRewrite(subQuery, stream);
        } else {
            return new QBSubQueryRewriteNoop(subQuery, stream);
        }
    }

    /** Responsible for capturing SubQuery rewrites and providing the rewritten query as SQL. */
    public static class QBSubQueryRewrite {
        HiveParserQBSubQuery subQuery;
        TokenRewriteStream stream;

        String joiningCondition;

        QBSubQueryRewrite(HiveParserQBSubQuery subQuery, TokenRewriteStream stream) {
            this.subQuery = subQuery;
            this.stream = stream;
        }

        public String getJoiningCondition() {
            return joiningCondition;
        }
    }

    /**
     * In the non explain code path, we don't need to track Query rewrites. All add fns during Plan
     * generation are Noops. If the get Rewrite methods are called, an UnsupportedOperationException
     * is thrown.
     */
    public static class QBSubQueryRewriteNoop extends QBSubQueryRewrite {

        QBSubQueryRewriteNoop(HiveParserQBSubQuery subQuery, TokenRewriteStream stream) {
            super(subQuery, stream);
        }

        @Override
        public final String getJoiningCondition() {
            throw new UnsupportedOperationException();
        }
    }
}
