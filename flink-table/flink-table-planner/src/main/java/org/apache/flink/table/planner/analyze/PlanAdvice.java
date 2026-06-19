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

package org.apache.flink.table.planner.analyze;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Plain POJO for advice provided by {@link PlanAnalyzer}. */
@Internal
public final class PlanAdvice {

    private final Kind kind;

    private final Scope scope;

    private final String content;

    public PlanAdvice(Kind kind, Scope scope, String content) {
        this.kind = kind;
        this.scope = scope;
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public Scope getScope() {
        return scope;
    }

    public Kind getKind() {
        return kind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PlanAdvice)) {
            return false;
        }
        PlanAdvice that = (PlanAdvice) o;
        return kind == that.kind && scope == that.scope && content.equals(that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, scope, content);
    }

    /** Categorize the semantics of a {@link PlanAdvice}. */
    @Internal
    public enum Kind {
        /** Indicate the potential risk. */
        WARNING,
        /** Indicate the potential optimization. */
        ADVICE
    }

    /** Categorize the scope of a {@link PlanAdvice}. */
    @Internal
    public enum Scope {
        /** Indicate the advice is not specific to a {@link org.apache.calcite.rel.RelNode}. */
        QUERY_LEVEL,
        /** Indicate the advice is specific to a {@link org.apache.calcite.rel.RelNode}. */
        NODE_LEVEL
    }
}
