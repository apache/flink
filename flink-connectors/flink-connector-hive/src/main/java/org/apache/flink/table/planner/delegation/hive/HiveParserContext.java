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

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Counterpart of hive's org.apache.hadoop.hive.ql.Context. */
public class HiveParserContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserContext.class);

    private final Configuration conf;
    protected int pathid = 10000;
    private TokenRewriteStream tokenRewriteStream;
    // Holds the qualified name to tokenRewriteStream for the views
    // referenced by the query. This is used to rewrite the view AST
    // with column masking and row filtering policies.
    private final Map<String, TokenRewriteStream> viewsTokenRewriteStreams;

    /**
     * These ops require special handling in various places (note that Insert into Acid table is in
     * OTHER category).
     */
    public enum Operation {
        UPDATE,
        DELETE,
        MERGE,
        OTHER
    }

    /** DestClausePrefix. */
    public enum DestClausePrefix {
        INSERT("insclause-"),
        UPDATE("updclause-"),
        DELETE("delclause-");
        private final String prefix;

        DestClausePrefix(String prefix) {
            this.prefix = prefix;
        }

        public String toString() {
            return prefix;
        }
    }

    /** The suffix is always relative to a given ASTNode. */
    public DestClausePrefix getDestNamePrefix(ASTNode curNode) {
        assert curNode != null : "must supply curNode";
        assert curNode.getType() == HiveASTParser.TOK_INSERT_INTO
                || curNode.getType() == HiveASTParser.TOK_DESTINATION;
        return DestClausePrefix.INSERT;
    }

    /**
     * Create a HiveParserContext with a given executionId. ExecutionId, together with user name and
     * conf, will determine the temporary directory locations.
     */
    public HiveParserContext(Configuration conf) {
        this.conf = conf;
        viewsTokenRewriteStreams = new HashMap<>();
    }

    public Path getMRTmpPath(URI uri) {
        return null;
    }

    /**
     * Set the token rewrite stream being used to parse the current top-level SQL statement. Note
     * that this should <b>not</b> be used for other parsing activities; for example, when we
     * encounter a reference to a view, we switch to a new stream for parsing the stored view
     * definition from the catalog, but we don't clobber the top-level stream in the context.
     *
     * @param tokenRewriteStream the stream being used
     */
    public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
        assert this.tokenRewriteStream == null;
        this.tokenRewriteStream = tokenRewriteStream;
    }

    /**
     * @return the token rewrite stream being used to parse the current top-level SQL statement, or
     *     null if it isn't available (e.g. for parser tests)
     */
    public TokenRewriteStream getTokenRewriteStream() {
        return tokenRewriteStream;
    }

    public void addViewTokenRewriteStream(
            String viewFullyQualifiedName, TokenRewriteStream tokenRewriteStream) {
        viewsTokenRewriteStreams.put(viewFullyQualifiedName, tokenRewriteStream);
    }

    public Configuration getConf() {
        return conf;
    }
}
