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

package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.NlsString;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;

/** The command to stop a flink job. */
public class SqlStopJob extends SqlCall {

    public static final SqlOperator OPERATOR =
            new SqlSpecialOperator("STOP JOB", SqlKind.OTHER_DDL);

    private final SqlCharStringLiteral jobId;

    private final boolean isWithDrain;

    private final boolean isWithSavepoint;

    public SqlStopJob(
            SqlParserPos pos,
            SqlCharStringLiteral jobId,
            boolean isWithSavepoint,
            boolean isWithDrain) {
        super(pos);
        this.jobId = jobId;
        this.isWithSavepoint = isWithSavepoint;
        this.isWithDrain = isWithDrain;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STOP");
        writer.keyword("JOB");
        jobId.unparse(writer, leftPrec, rightPrec);
        if (isWithSavepoint) {
            writer.keyword("WITH SAVEPOINT");
        }
        if (isWithDrain) {
            writer.keyword("WITH DRAIN");
        }
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(jobId);
    }

    public String getId() {
        return jobId.getValueAs(NlsString.class).getValue();
    }

    public boolean isWithSavepoint() {
        return isWithSavepoint;
    }

    public boolean isWithDrain() {
        return isWithDrain;
    }
}
