package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

public class MSSQLRowConverter extends AbstractJdbcRowConverter {
    public MSSQLRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    public String converterName() {
        return "MSSQL";
    }
}
