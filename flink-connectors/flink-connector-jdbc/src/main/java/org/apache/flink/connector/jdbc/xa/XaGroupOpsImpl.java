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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.xa.XaFacade.TransientXaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@Internal
class XaGroupOpsImpl implements XaGroupOps {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(XaGroupOpsImpl.class);

    private final XaFacade xaFacade;

    XaGroupOpsImpl(XaFacade xaFacade) {
        this.xaFacade = xaFacade;
    }

    @Override
    public GroupXaOperationResult<CheckpointAndXid> commit(
            List<CheckpointAndXid> xids, boolean allowOutOfOrderCommits, int maxCommitAttempts) {
        GroupXaOperationResult<CheckpointAndXid> result = new GroupXaOperationResult<>();
        int origSize = xids.size();
        LOG.debug("commit {} transactions", origSize);
        for (Iterator<CheckpointAndXid> i = xids.iterator();
                i.hasNext() && (result.hasNoFailures() || allowOutOfOrderCommits); ) {
            CheckpointAndXid x = i.next();
            i.remove();
            try {
                xaFacade.commit(x.xid, x.restored);
                result.succeeded(x);
            } catch (TransientXaException e) {
                result.failedTransiently(x.withAttemptsIncremented(), e);
            } catch (Exception e) {
                result.failed(x, e);
            }
        }
        result.getForRetry().addAll(xids);
        result.throwIfAnyFailed("commit");
        throwIfAnyReachedMaxAttempts(result, maxCommitAttempts);
        result.getTransientFailure()
                .ifPresent(
                        f ->
                                LOG.warn(
                                        "failed to commit {} transactions out of {} (keep them to retry later)",
                                        result.getForRetry().size(),
                                        origSize,
                                        f));
        return result;
    }

    @Override
    public GroupXaOperationResult<Xid> failOrRollback(Collection<Xid> xids) {
        GroupXaOperationResult<Xid> result = new GroupXaOperationResult<>();
        if (xids.isEmpty()) {
            return result;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("rolling back {} transactions: {}", xids.size(), xids);
        }
        for (Xid x : xids) {
            try {
                xaFacade.failOrRollback(x);
                result.succeeded(x);
            } catch (TransientXaException e) {
                LOG.info("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failedTransiently(x, e);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failed(x, e);
            }
        }
        if (!result.getForRetry().isEmpty()) {
            LOG.info("failed to roll back {} transactions", result.getForRetry().size());
        }
        return result;
    }

    @Override
    public void recoverAndRollback() {
        Collection<Xid> recovered = xaFacade.recover();
        if (recovered.isEmpty()) {
            return;
        }
        LOG.warn("rollback {} recovered transactions", recovered.size());
        for (Xid xid : recovered) {
            try {
                xaFacade.rollback(xid);
            } catch (Exception e) {
                LOG.info("unable to rollback recovered transaction, xid={}", xid, e);
            }
        }
    }

    private static void throwIfAnyReachedMaxAttempts(
            GroupXaOperationResult<CheckpointAndXid> result, int maxAttempts) {
        List<CheckpointAndXid> reached = null;
        for (CheckpointAndXid x : result.getForRetry()) {
            if (x.attempts >= maxAttempts) {
                if (reached == null) {
                    reached = new ArrayList<>();
                }
                reached.add(x);
            }
        }
        if (reached != null) {
            throw new RuntimeException(
                    String.format(
                            "reached max number of commit attempts (%d) for transactions: %s",
                            maxAttempts, reached));
        }
    }
}
