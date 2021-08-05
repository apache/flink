package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for
 * Clickhouse.
 */
public class ClickhouseRowConverter extends AbstractJdbcRowConverter {

    private static final long serialVersionUID = 1L;

    @Override
    public String converterName() {
        return "Clickhouse";
    }

    public ClickhouseRowConverter(RowType rowType) {
        super(rowType);
    }
}
