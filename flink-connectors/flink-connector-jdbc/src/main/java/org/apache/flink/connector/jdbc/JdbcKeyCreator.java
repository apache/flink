package org.apache.flink.connector.jdbc;

import java.io.Serializable;
import java.util.function.Function;

public interface JdbcKeyCreator<T> extends Function<T, String>, Serializable {}
