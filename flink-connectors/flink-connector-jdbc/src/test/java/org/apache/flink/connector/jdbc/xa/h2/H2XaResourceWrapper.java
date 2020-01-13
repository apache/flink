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

package org.apache.flink.connector.jdbc.xa.h2;

import org.apache.flink.util.function.ThrowingRunnable;

import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbcx.JdbcXAConnection;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * Wraps H2 {@link XAResource} to:
 *
 * <ol>
 *   <li>reset <code>currentTransaction</code> field after a call to {@link XAResource#prepare
 *       prepare}. This allows to {@link XAResource#start start} a new transaction after preparing
 *       the current one. (see this <a
 *       href="http://h2-database.66688.n3.nabble.com/Possible-XA-bug-and-fix-td3303095.html">discussion</a>)
 *   <li>prevent {@link NullPointerException} when there is no active XA transaction in {@link
 *       XAResource#start start}, {@link XAResource#end end} (and throw appropriate {@link
 *       XAException} instead)
 * </ol>
 *
 * <p>These fixes are just enough to test Flink XA-related functionality. They aren't enough for
 * proper XA support.
 *
 * <p>TODO: fix the issue in the upstream.
 */
public class H2XaResourceWrapper implements XAResource {

    private static final Field CURRENT_TRANSACTION_FIELD;
    private static final Field IS_PREPARED_FIELD;
    private static final Field PHY;

    static {
        try {
            CURRENT_TRANSACTION_FIELD =
                    JdbcXAConnection.class.getDeclaredField("currentTransaction");
            CURRENT_TRANSACTION_FIELD.setAccessible(true);
            IS_PREPARED_FIELD = JdbcXAConnection.class.getDeclaredField("prepared");
            IS_PREPARED_FIELD.setAccessible(true);
            PHY = JdbcXAConnection.class.getDeclaredField("physicalConn");
            PHY.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private final XAResource wrapped;
    private final Set<Xid> forgotten = new HashSet<>();

    H2XaResourceWrapper(XAResource wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        ensureNotForgotten(xid);
        Object currentTransaction = getCurrentTransaction();
        if (onePhase) {
            if (currentTransaction == null) {
                // prevent NPE in wrapped
                throw new XAException(XAException.XAER_PROTO);
            } else if (!currentTransaction.equals(xid)) {
                // guard from committing a wrong transaction:
                // if onePhase=true then the underlying impl. commits current tx, not the one
                // specified by xid
                // (according to spec it should commit the ended but not prepared one - Section
                // 2.3.2)
                throw new XAException(XAException.XAER_INVAL);
            }
        }
        finalizeTx(() -> wrapped.commit(xid, onePhase));
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        ensureNotForgotten(xid);
        setPrepared(
                true); // if prepared=false then the underlying impl. will rollback current tx, not
        // the one identified by xid
        finalizeTx(() -> wrapped.rollback(xid));
    }

    @Override
    public void end(Xid xid, int i) throws XAException {
        Object currentTransaction = getCurrentTransaction();
        if (currentTransaction == null) {
            // prevent NPE in wrapped
            throw new XAException(XAException.XAER_PROTO);
        } else if (!currentTransaction.equals(xid)) {
            // fix error code according to spec p.37:
            // The argument xid must point to the same XID that was either passed to the xa_start()
            // call or returned
            // from the ax_reg() call that established the thread’s association; otherwise, an
            // error, [XAER_NOTA], is
            // returned
            throw new XAException(XAException.XAER_NOTA);
        }
        wrapped.end(xid, i);
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        // prevent NPE in wrapped and preparing the wrong transaction
        if (getCurrentTransaction() == null || !xid.equals(getCurrentTransaction())) {
            throw new UnsupportedOperationException(
                    "preparing a transaction that is  not currently active not supported");
        }
        int ret = wrapped.prepare(xid);
        setCurrentTransaction(
                null); // prevent failure in start(); don't reset in in end() because end() actually
        // does nothing
        return ret;
    }

    @Override
    public void forget(Xid xid) throws XAException {
        wrapped.forget(xid);
        forgotten.add(xid); // underlying implementation doesn't actually forget transactions
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return wrapped.getTransactionTimeout();
    }

    @Override
    public boolean isSameRM(XAResource xaResource) throws XAException {
        return wrapped.isSameRM(xaResource);
    }

    @Override
    public Xid[] recover(int i) throws XAException {
        return wrapped.recover(i);
    }

    @Override
    public boolean setTransactionTimeout(int i) throws XAException {
        return wrapped.setTransactionTimeout(i);
    }

    @Override
    public void start(Xid xid, int i) throws XAException {
        wrapped.start(xid, i);
        forgotten.remove(xid);
    }

    private boolean isPrepared() {
        try {
            return (boolean) IS_PREPARED_FIELD.get(wrapped);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Object getCurrentTransaction() {
        try {
            return CURRENT_TRANSACTION_FIELD.get(wrapped);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureInTx() throws XAException {
        if (getCurrentTransaction() == null) {
            throw new XAException(XAException.XAER_OUTSIDE);
        }
    }

    private void setCurrentTransaction(Object currentTransaction) {
        try {
            CURRENT_TRANSACTION_FIELD.set(wrapped, currentTransaction);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void setPrepared(boolean prepared) {
        try {
            IS_PREPARED_FIELD.set(wrapped, prepared);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private XAException repackageXaException(XAException e) {
        if (e.getCause() instanceof SQLException) {
            if (((SQLException) e.getCause()).getErrorCode() == ErrorCode.TRANSACTION_NOT_FOUND_1) {
                return new XAException(XAException.XAER_NOTA);
            }
        }
        return e;
    }

    private JdbcConnection getJdbcConnection() {
        try {
            return (JdbcConnection) PHY.get(wrapped);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private void ensureNotForgotten(Xid xid) throws XAException {
        if (forgotten.contains(xid)) {
            throw new XAException(XAException.XAER_NOTA);
        }
    }

    private void finalizeTx(ThrowingRunnable<XAException> runnable) throws XAException {
        // underlying implementation sets autocommit on just after commit or rollback
        // to prevent the actual change we turn it on the lower level and then revert
        withSessionAutocommitOn(
                () ->
                        // underlying implementation nulls out current transaction just after commit
                        // or rollback
                        // which prevents it from being prepared afterwards
                        withCurrentTransaction(runnable));
    }

    private void withCurrentTransaction(ThrowingRunnable<XAException> runnable) throws XAException {
        Object current = getCurrentTransaction();
        try {
            runnable.run();
        } catch (XAException e) {
            throw repackageXaException(e);
        } finally {
            setCurrentTransaction(current);
        }
    }

    private void withSessionAutocommitOn(ThrowingRunnable<XAException> runnable)
            throws XAException {
        JdbcConnection conn = getJdbcConnection();
        boolean autoCommit = conn.getSession().getAutoCommit();
        conn.getSession().setAutoCommit(true);
        try {
            runnable.run();
        } finally {
            conn.getSession().setAutoCommit(autoCommit);
        }
    }
}
