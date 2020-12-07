package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for Clickhouse.
 */
public class ClickhouseJdbcRowConverter extends AbstractJdbcRowConverter {
	@Override
	public String converterName() {
		return "Clickhouse";
	}

	public ClickhouseJdbcRowConverter(RowType rowType) {
		super(rowType);
	}
}
