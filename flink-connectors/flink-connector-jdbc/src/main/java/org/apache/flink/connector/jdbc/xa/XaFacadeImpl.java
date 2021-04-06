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
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static javax.transaction.xa.XAException.XAER_NOTA;
import static javax.transaction.xa.XAException.XAER_RMFAIL;
import static javax.transaction.xa.XAException.XA_HEURCOM;
import static javax.transaction.xa.XAException.XA_HEURHAZ;
import static javax.transaction.xa.XAException.XA_HEURMIX;
import static javax.transaction.xa.XAException.XA_HEURRB;
import static javax.transaction.xa.XAException.XA_RBBASE;
import static javax.transaction.xa.XAException.XA_RBTIMEOUT;
import static javax.transaction.xa.XAException.XA_RBTRANSIENT;
import static javax.transaction.xa.XAResource.TMENDRSCAN;
import static javax.transaction.xa.XAResource.TMNOFLAGS;
import static javax.transaction.xa.XAResource.TMSTARTRSCAN;

/** Default {@link XaFacade} implementation. */
@NotThreadSafe
@Internal
class XaFacadeImpl implements XaFacade {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(XaFacadeImpl.class);
    private static final Set<Integer> TRANSIENT_ERR_CODES =
            new HashSet<>(Arrays.asList(XA_RBTRANSIENT, XAER_RMFAIL));
    private static final Set<Integer> HEUR_ERR_CODES =
            new HashSet<>(Arrays.asList(XA_HEURRB, XA_HEURCOM, XA_HEURHAZ, XA_HEURMIX));
    private static final int MAX_RECOVER_CALLS = 100;

    private final Supplier<XADataSource> dataSourceSupplier;
    private final Integer timeoutSec;
    private transient XAResource xaResource;
    private transient Connection connection;
    private transient XAConnection xaConnection;

    /** @return a non-serializable instance. */
    static XaFacadeImpl fromXaDataSource(XADataSource ds) {
        return new XaFacadeImpl(() -> ds, empty());
    }

    XaFacadeImpl(Supplier<XADataSource> dataSourceSupplier, Optional<Integer> timeoutSec) {
        this.dataSourceSupplier = Preconditions.checkNotNull(dataSourceSupplier);
        this.timeoutSec = timeoutSec.orElse(null);
    }

    @Override
    public void open() throws SQLException {
        Preconditions.checkState(!isOpen(), "already connected");
        XADataSource ds = dataSourceSupplier.get();
        xaConnection = ds.getXAConnection();
        xaResource = xaConnection.getXAResource();
        if (timeoutSec != null) {
            try {
                xaResource.setTransactionTimeout(timeoutSec);
            } catch (XAException e) {
                throw new SQLException(e);
            }
        }
        connection = xaConnection.getConnection();
        connection.setReadOnly(false);
        connection.setAutoCommit(false);
        Preconditions.checkState(!connection.getAutoCommit());
    }

    @Override
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (xaConnection != null) {
            xaConnection.close();
            xaConnection = null;
        }
        xaResource = null;
    }

    @Override
    public Connection getConnection() {
        Preconditions.checkNotNull(connection);
        return connection;
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        return isOpen() && connection.isValid(connection.getNetworkTimeout());
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException {
        if (!isOpen()) {
            open();
        }
        return connection;
    }

    @Override
    public void closeConnection() {
        try {
            close();
        } catch (SQLException e) {
            LOG.warn("Connection close failed.", e);
        }
    }

    @Override
    public Connection reestablishConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start(Xid xid) {
        execute(Command.fromRunnable("start", xid, () -> xaResource.start(xid, TMNOFLAGS)));
    }

    @Override
    public void endAndPrepare(Xid xid) {
        execute(Command.fromRunnable("end", xid, () -> xaResource.end(xid, XAResource.TMSUCCESS)));
        int prepResult = execute(new Command<>("prepare", of(xid), () -> xaResource.prepare(xid)));
        if (prepResult == XAResource.XA_RDONLY) {
            throw new EmptyXaTransactionException(xid);
        } else if (prepResult != XAResource.XA_OK) {
            throw new FlinkRuntimeException(
                    formatErrorMessage("prepare", of(xid), empty(), "response: " + prepResult));
        }
    }

    @Override
    public void failOrRollback(Xid xid) {
        execute(
                Command.fromRunnable(
                        "end (fail)",
                        xid,
                        () -> xaResource.end(xid, XAResource.TMFAIL),
                        err -> {
                            if (err.errorCode >= XA_RBBASE) {
                                rollback(xid);
                            } else {
                                LOG.warn(
                                        formatErrorMessage(
                                                "end (fail)", of(xid), of(err.errorCode)));
                            }
                        }));
    }

    @Override
    public void commit(Xid xid, boolean ignoreUnknown) {
        execute(
                Command.fromRunnableRecoverByWarn(
                        "commit",
                        xid,
                        () ->
                                xaResource.commit(
                                        xid,
                                        false /* not onePhase because the transaction should be prepared already */),
                        e -> buildCommitErrorDesc(e, ignoreUnknown)));
    }

    @Override
    public void rollback(Xid xid) {
        execute(
                Command.fromRunnableRecoverByWarn(
                        "rollback",
                        xid,
                        () -> xaResource.rollback(xid),
                        this::buildRollbackErrorDesc));
    }

    private void forget(Xid xid) {
        execute(
                Command.fromRunnableRecoverByWarn(
                        "forget",
                        xid,
                        () -> xaResource.forget(xid),
                        e -> of("manual cleanup may be required")));
    }

    @Override
    public Collection<Xid> recover() {
        return execute(
                new Command<>(
                        "recover",
                        empty(),
                        () -> {
                            List<Xid> list = recover(TMSTARTRSCAN);
                            try {
                                for (int i = 0; list.addAll(recover(TMNOFLAGS)); i++) {
                                    // H2 sometimes returns same tx list here - should probably use
                                    // recover(TMSTARTRSCAN | TMENDRSCAN)
                                    Preconditions.checkState(
                                            i < MAX_RECOVER_CALLS, "too many xa_recover() calls");
                                }
                            } finally {
                                recover(TMENDRSCAN);
                            }
                            return list;
                        }));
    }

    @Override
    public boolean isOpen() {
        return xaResource != null;
    }

    private List<Xid> recover(int flags) throws XAException {
        return Arrays.asList(xaResource.recover(flags));
    }

    private <T> T execute(Command<T> cmd) throws FlinkRuntimeException {
        Preconditions.checkState(isOpen(), "not connected");
        LOG.debug("{}, xid={}", cmd.name, cmd.xid);
        try {
            T result = cmd.callable.call();
            LOG.trace("{} succeeded , xid={}", cmd.name, cmd.xid);
            return result;
        } catch (XAException e) {
            if (HEUR_ERR_CODES.contains(e.errorCode)) {
                cmd.xid.ifPresent(this::forget);
            }
            return cmd.recover.apply(e).orElseThrow(() -> wrapException(cmd.name, cmd.xid, e));
        } catch (FlinkRuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw wrapException(cmd.name, cmd.xid, e);
        }
    }

    private static class Command<T> {
        private final String name;
        private final Optional<Xid> xid;
        private final Callable<T> callable;
        private final Function<XAException, Optional<T>> recover;

        static Command<Object> fromRunnable(
                String action, Xid xid, ThrowingRunnable<XAException> runnable) {
            return fromRunnable(
                    action,
                    xid,
                    runnable,
                    e -> {
                        throw wrapException(action, of(xid), e);
                    });
        }

        static Command<Object> fromRunnableRecoverByWarn(
                String action,
                Xid xid,
                ThrowingRunnable<XAException> runnable,
                Function<XAException, Optional<String>> err2msg) {
            return fromRunnable(
                    action,
                    xid,
                    runnable,
                    e ->
                            LOG.warn(
                                    formatErrorMessage(
                                            action,
                                            of(xid),
                                            of(e.errorCode),
                                            err2msg.apply(e)
                                                    .orElseThrow(
                                                            () ->
                                                                    wrapException(
                                                                            action, of(xid), e)))));
        }

        private static Command<Object> fromRunnable(
                String action,
                Xid xid,
                ThrowingRunnable<XAException> runnable,
                Consumer<XAException> recover) {
            return new Command<>(
                    action,
                    of(xid),
                    () -> {
                        runnable.run();
                        return null;
                    },
                    e -> {
                        recover.accept(e);
                        return Optional.of("");
                    });
        }

        private Command(String name, Optional<Xid> xid, Callable<T> callable) {
            this(name, xid, callable, e -> empty());
        }

        private Command(
                String name,
                Optional<Xid> xid,
                Callable<T> callable,
                Function<XAException, Optional<T>> recover) {
            this.name = name;
            this.xid = xid;
            this.callable = callable;
            this.recover = recover;
        }
    }

    private static FlinkRuntimeException wrapException(
            String action, Optional<Xid> xid, Exception ex) {
        if (ex instanceof XAException) {
            XAException xa = (XAException) ex;
            if (TRANSIENT_ERR_CODES.contains(xa.errorCode)) {
                throw new TransientXaException(xa);
            } else {
                throw new FlinkRuntimeException(
                        formatErrorMessage(action, xid, of(xa.errorCode), xa.getMessage()));
            }
        } else {
            throw new FlinkRuntimeException(
                    formatErrorMessage(action, xid, empty(), ex.getMessage()), ex);
        }
    }

    private Optional<String> buildCommitErrorDesc(XAException err, boolean ignoreUnknown) {
        if (err.errorCode == XA_HEURCOM) {
            return Optional.of("transaction was heuristically committed earlier");
        } else if (ignoreUnknown && err.errorCode == XAER_NOTA) {
            return Optional.of("transaction is unknown to RM (ignoring)");
        } else {
            return empty();
        }
    }

    private Optional<String> buildRollbackErrorDesc(XAException err) {
        if (err.errorCode == XA_HEURRB) {
            return Optional.of("transaction was already heuristically rolled back");
        } else if (err.errorCode >= XA_RBBASE) {
            return Optional.of("transaction was already marked for rollback");
        } else {
            return empty();
        }
    }

    private static String formatErrorMessage(
            String action, Optional<Xid> xid, Optional<Integer> errorCode, String... more) {
        return String.format(
                "unable to %s%s%s%s",
                action,
                xid.map(x -> " XA transaction, xid: " + x).orElse(""),
                errorCode
                        .map(code -> String.format(", error %d: %s", code, descError(code)))
                        .orElse(""),
                more == null || more.length == 0 ? "" : ". " + Arrays.toString(more));
    }

    /** @return error description from {@link XAException} javadoc from to ease debug. */
    private static String descError(int code) {
        switch (code) {
            case XA_HEURCOM:
                return "heuristic commit decision was made";
            case XAException.XA_HEURHAZ:
                return "heuristic decision may have been made";
            case XAException.XA_HEURMIX:
                return "heuristic mixed decision was made";
            case XA_HEURRB:
                return "heuristic rollback decision was made";
            case XAException.XA_NOMIGRATE:
                return "the transaction resumption must happen where the suspension occurred";
            case XAException.XA_RBCOMMFAIL:
                return "rollback happened due to a communications failure";
            case XAException.XA_RBDEADLOCK:
                return "rollback happened because deadlock was detected";
            case XAException.XA_RBINTEGRITY:
                return "rollback happened because an internal integrity check failed";
            case XAException.XA_RBOTHER:
                return "rollback happened for some reason not fitting any of the other rollback error codes";
            case XAException.XA_RBPROTO:
                return "rollback happened due to a protocol error in the resource manager";
            case XAException.XA_RBROLLBACK:
                return "rollback happened for an unspecified reason";
            case XA_RBTIMEOUT:
                return "rollback happened because of a timeout";
            case XA_RBTRANSIENT:
                return "rollback happened due to a transient failure";
            case XAException.XA_RDONLY:
                return "the transaction branch was read-only, and has already been committed";
            case XAException.XA_RETRY:
                return "the method invoked returned without having any effect, and that it may be invoked again";
            case XAException.XAER_ASYNC:
                return "an asynchronous operation is outstanding";
            case XAException.XAER_DUPID:
                return "Xid given as an argument is already known to the resource manager";
            case XAException.XAER_INVAL:
                return "invalid arguments were passed";
            case XAER_NOTA:
                return "Xid is not valid";
            case XAException.XAER_OUTSIDE:
                return "the resource manager is doing work outside the global transaction";
            case XAException.XAER_PROTO:
                return "protocol error";
            case XAException.XAER_RMERR:
                return "resource manager error has occurred";
            case XAException.XAER_RMFAIL:
                return "the resource manager has failed and is not available";
            default:
                return "";
        }
    }
}
