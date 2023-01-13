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

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.QBExpr. */
public class HiveParserQBExpr {

    private static final Logger LOG = LoggerFactory.getLogger(HiveParserQBExpr.class);

    /** Opcode. */
    public enum Opcode {
        NULLOP,
        UNION,
        INTERSECT,
        INTERSECTALL,
        EXCEPT,
        EXCEPTALL,
        DIFF
    }

    private Opcode opcode;
    private HiveParserQBExpr qbexpr1;
    private HiveParserQBExpr qbexpr2;
    private HiveParserQB qb;
    private String alias;

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public HiveParserQBExpr(String alias) {
        this.alias = alias;
    }

    public void setQB(HiveParserQB qb) {
        this.qb = qb;
    }

    public void setOpcode(Opcode opcode) {
        this.opcode = opcode;
    }

    public void setQBExpr1(HiveParserQBExpr qbexpr) {
        qbexpr1 = qbexpr;
    }

    public void setQBExpr2(HiveParserQBExpr qbexpr) {
        qbexpr2 = qbexpr;
    }

    public HiveParserQB getQB() {
        return qb;
    }

    public Opcode getOpcode() {
        return opcode;
    }

    public HiveParserQBExpr getQBExpr1() {
        return qbexpr1;
    }

    public HiveParserQBExpr getQBExpr2() {
        return qbexpr2;
    }

    public void print(String msg) {
        if (opcode == Opcode.NULLOP) {
            LOG.info(msg + "start qb = " + qb);
            qb.print(msg + " ");
            LOG.info(msg + "end qb = " + qb);
        } else {
            LOG.info(msg + "start qbexpr1 = " + qbexpr1);
            qbexpr1.print(msg + " ");
            LOG.info(msg + "end qbexpr1 = " + qbexpr1);
            LOG.info(msg + "start qbexpr2 = " + qbexpr2);
            qbexpr2.print(msg + " ");
            LOG.info(msg + "end qbexpr2 = " + qbexpr2);
        }
    }

    public boolean isSimpleSelectQuery() {
        if (qb != null) {
            return qb.isSimpleSelectQuery();
        }
        return qbexpr1.isSimpleSelectQuery() && qbexpr2.isSimpleSelectQuery();
    }

    /**
     * returns true, if the query block contains any query, or subquery without a source table. Like
     * select current_user(), select current_database()
     *
     * @return true, if the query block contains any query without a source table
     */
    public boolean containsQueryWithoutSourceTable() {
        if (qb != null) {
            return qb.containsQueryWithoutSourceTable();
        } else {
            return qbexpr1.containsQueryWithoutSourceTable()
                    || qbexpr2.containsQueryWithoutSourceTable();
        }
    }
}
