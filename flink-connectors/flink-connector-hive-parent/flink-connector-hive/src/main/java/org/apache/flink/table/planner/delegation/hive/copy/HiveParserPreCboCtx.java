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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Counterpart of hive's CalcitePlanner.PreCboCtx. */
public class HiveParserPreCboCtx extends HiveParserPlannerContext {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserPreCboCtx.class);

    /** Type. */
    public enum Type {
        NONE,
        INSERT,
        MULTI_INSERT,
        CTAS,
        VIEW,
        UNEXPECTED
    }

    public HiveParserASTNode nodeOfInterest;
    public Type type = Type.NONE;

    private void set(Type type, HiveParserASTNode ast) {
        if (this.type != Type.NONE) {
            LOG.warn(
                    "Setting "
                            + type
                            + " when already "
                            + this.type
                            + "; node "
                            + ast.dump()
                            + " vs old node "
                            + nodeOfInterest.dump());
            this.type = Type.UNEXPECTED;
            return;
        }
        this.type = type;
        this.nodeOfInterest = ast;
    }

    @Override
    void setCTASToken(HiveParserASTNode child) {
        set(Type.CTAS, child);
    }

    @Override
    void setViewToken(HiveParserASTNode child) {
        set(Type.VIEW, child);
    }

    @Override
    void setInsertToken(HiveParserASTNode ast, boolean isTmpFileDest) {
        if (!isTmpFileDest) {
            set(Type.INSERT, ast);
        }
    }

    @Override
    void setMultiInsertToken(HiveParserASTNode child) {
        set(Type.MULTI_INSERT, child);
    }

    @Override
    void resetToken() {
        this.type = Type.NONE;
        this.nodeOfInterest = null;
    }
}
