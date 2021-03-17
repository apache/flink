/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.xa;

import javax.annotation.concurrent.ThreadSafe;
import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import static java.util.Collections.unmodifiableCollection;

/** Thread-safe (assuming immutable {@link Xid} implementation). */
@ThreadSafe
class JdbcXaSinkFunctionState {
    private final Collection<CheckpointAndXid> prepared;
    private final Collection<Xid> hanging;

    static JdbcXaSinkFunctionState empty() {
        return new JdbcXaSinkFunctionState(Collections.emptyList(), Collections.emptyList());
    }

    static JdbcXaSinkFunctionState of(
            Collection<CheckpointAndXid> prepared, Collection<Xid> hanging) {
        return new JdbcXaSinkFunctionState(
                unmodifiableCollection(new ArrayList<>(prepared)),
                unmodifiableCollection(new ArrayList<>(hanging)));
    }

    private JdbcXaSinkFunctionState(
            Collection<CheckpointAndXid> prepared, Collection<Xid> hanging) {
        this.prepared = prepared;
        this.hanging = hanging;
    }

    /**
     * @return immutable collection of prepared XA transactions to {@link
     *     javax.transaction.xa.XAResource#commit commit}.
     */
    public Collection<CheckpointAndXid> getPrepared() {
        return prepared;
    }

    /**
     * @return immutable collection of XA transactions to {@link
     *     javax.transaction.xa.XAResource#rollback rollback} (if they were prepared) or {@link
     *     javax.transaction.xa.XAResource#end end} (if they were only started).
     */
    Collection<Xid> getHanging() {
        return hanging;
    }

    @Override
    public String toString() {
        return "prepared=" + prepared + ", hanging=" + hanging;
    }
}
