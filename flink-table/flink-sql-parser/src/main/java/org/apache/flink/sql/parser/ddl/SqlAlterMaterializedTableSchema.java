package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.SqlUnparseUtils;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Abstract class to describe statements which are used to alter schema for materialized tables. */
public abstract class SqlAlterMaterializedTableSchema extends SqlAlterMaterializedTable
        implements ExtendedSqlNode {

    protected final SqlNodeList columnList;
    protected final List<SqlTableConstraint> constraints;
    protected final @Nullable SqlWatermark watermark;

    public SqlAlterMaterializedTableSchema(
            SqlParserPos pos,
            SqlIdentifier materializedTableName,
            SqlNodeList columnList,
            List<SqlTableConstraint> constraints,
            @Nullable SqlWatermark sqlWatermark) {
        super(pos, materializedTableName);
        this.columnList = columnList;
        this.constraints = constraints;
        this.watermark = sqlWatermark;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(
                name, columnList, new SqlNodeList(constraints, SqlParserPos.ZERO), watermark);
    }

    @Override
    public void validate() throws SqlValidateException {
        SqlConstraintValidator.validateAndChangeColumnNullability(constraints, getColumns());
    }

    public SqlNodeList getColumnPositions() {
        return columnList;
    }

    public Optional<SqlWatermark> getWatermark() {
        return Optional.ofNullable(watermark);
    }

    public List<SqlTableConstraint> getConstraints() {
        return constraints;
    }

    private SqlNodeList getColumns() {
        return new SqlNodeList(
                columnList.getList().stream()
                        .map(columnPos -> ((SqlTableColumnPosition) columnPos).getColumn())
                        .collect(Collectors.toList()),
                SqlParserPos.ZERO);
    }

    protected abstract String getAlterOperation();

    @Override
    public void unparseAlterOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        super.unparseAlterOperation(writer, leftPrec, rightPrec);
        writer.keyword(getAlterOperation());
        SqlUnparseUtils.unparseTableSchema(
                columnList, constraints, watermark, writer, leftPrec, rightPrec);
    }

    public static class SqlAlterMaterializedTableAddSchema extends SqlAlterMaterializedTableSchema {
        public SqlAlterMaterializedTableAddSchema(
                SqlParserPos pos,
                SqlIdentifier materializedTableName,
                SqlNodeList columnList,
                List<SqlTableConstraint> constraints,
                @Nullable SqlWatermark sqlWatermark) {
            super(pos, materializedTableName, columnList, constraints, sqlWatermark);
        }

        @Override
        protected String getAlterOperation() {
            return "ADD";
        }
    }

    public static class SqlAlterMaterializedTableModifySchema
            extends SqlAlterMaterializedTableSchema {
        public SqlAlterMaterializedTableModifySchema(
                SqlParserPos pos,
                SqlIdentifier materializedTableName,
                SqlNodeList columnList,
                List<SqlTableConstraint> constraints,
                @Nullable SqlWatermark sqlWatermark) {
            super(pos, materializedTableName, columnList, constraints, sqlWatermark);
        }

        @Override
        protected String getAlterOperation() {
            return "MODIFY";
        }
    }
}
