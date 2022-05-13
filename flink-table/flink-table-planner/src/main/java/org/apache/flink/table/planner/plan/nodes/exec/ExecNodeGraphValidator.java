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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;

/** This class validates the {@link ExecNodeGraph}. */
public class ExecNodeGraphValidator extends AbstractExecNodeExactlyOnceVisitor {

    @Override
    protected void visitNode(ExecNode<?> node) {
        if (node instanceof StreamExecLookupJoin) {
            // We need to do this as TemporalTableSourceSpec might be initiated with legacy tables.
            StreamExecLookupJoin streamExecLookupJoin = (StreamExecLookupJoin) node;
            if (streamExecLookupJoin.getTemporalTableSourceSpec().getTableSourceSpec() == null) {
                throw new ValidationException("TemporalTableSourceSpec can not be serialized.");
            }
        }
        super.visitInputs(node);
    }
}
