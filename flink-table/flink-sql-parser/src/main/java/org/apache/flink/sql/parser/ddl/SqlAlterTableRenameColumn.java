package org.apache.flink.sql.parser.ddl;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * ALTER TABLE [[catalogName.] dataBasesName].tableName
 * RENAME  originColumnName TO newColumnName
 */
public class SqlAlterTableRenameColumn extends SqlAlterTable {

    private final SqlIdentifier originColumnNameIdentifier;
    private final SqlIdentifier newColumnNameIdentifier;

    public SqlAlterTableRenameColumn(SqlParserPos pos,
                                     SqlIdentifier tableName,
                                     SqlIdentifier originColumnName,
                                     SqlIdentifier newColumnName) {
        super(pos, tableName, null);
        this.originColumnNameIdentifier = originColumnName;
        this.newColumnNameIdentifier = newColumnName;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableIdentifier, originColumnNameIdentifier, newColumnNameIdentifier);
    }

    public SqlIdentifier getOriginColumnNameIdentifier() {
        return originColumnNameIdentifier;
    }

    public SqlIdentifier getNewColumnNameIdentifier() {
        return newColumnNameIdentifier;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER TABLE");
        tableIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("RENAME");
        originColumnNameIdentifier.unparse(writer, leftPrec, rightPrec);
        writer.keyword("TO");
        newColumnNameIdentifier.unparse(writer, leftPrec, rightPrec);
    }
}
