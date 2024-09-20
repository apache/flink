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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.RestoreTestBase;
import org.apache.flink.table.test.program.TableTestProgram;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/** Tests for verifying {@link StreamExecOverAggregate}. */
public class OverWindowRestoreTest extends RestoreTestBase {
    public OverWindowRestoreTest() {
        super(StreamExecOverAggregate.class);
    }

    @Override
    protected Stream<String> getSavepointPaths(
            TableTestProgram program, ExecNodeMetadata metadata) {
        if (metadata.version() == 1) {
            return Stream.concat(
                    super.getSavepointPaths(program, metadata),
                    // See src/test/resources/restore-tests/stream-exec-over-aggregate_1/over
                    // -aggregate-lag/1.18/savepoint/OverWindowRestoreTest how the savepoint was
                    // generated
                    Stream.of(getSavepointPath(program, metadata, FlinkVersion.v1_18)));
        } else {
            return super.getSavepointPaths(program, metadata);
        }
    }

    @Override
    public List<TableTestProgram> programs() {
        return Collections.singletonList(OverWindowTestPrograms.LAG_OVER_FUNCTION);
    }
}
