package org.apache.flink.connector.jdbc;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.function.Function;

/**
 * Sets {@link PreparedStatement} table name to use in JDBC Sink.
 *
 * @see JdbcBatchStatementExecutor
 */
@PublicEvolving
public interface JdbcStatementFactory<T> extends Function<T, String>, Serializable {}
