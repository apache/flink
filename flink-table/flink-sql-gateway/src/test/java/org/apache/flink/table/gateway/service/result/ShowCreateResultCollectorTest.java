package org.apache.flink.table.gateway.service.result;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.internal.CachedPlan;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TestSchemaResolver;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShowCreateResultCollectorTest {
    @Test
    void testMaskingWhenEnabled() {
        var collector = new ShowCreateResultCollector(true, new String[] {"password", "token"});
        var result =
                createShowCreateTableResult(
                        "CREATE TABLE t WITH ('password' = 'secret', 'token' = 'XYZ')");

        var actual = collector.collect(result);

        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).getString(0).toString())
                .isEqualTo("CREATE TABLE t WITH ('password' = '****', 'token' = '****')");
    }

    @Test
    void testNoMaskingWhenDisabled() {
        var collector = new ShowCreateResultCollector(false, new String[] {"password"});
        var result = createShowCreateTableResult("CREATE TABLE t WITH ('password' = 'secret')");

        List<RowData> actual = collector.collect(result);

        assertThat(actual.get(0).getString(0).toString())
                .isEqualTo("CREATE TABLE t WITH ('password' = 'secret')");
    }

    @Test
    void testMatchOptionUsingContains() {
        var collector = new ShowCreateResultCollector(true, new String[] {"pass"});

        TableResultInternal result =
                createShowCreateTableResult(
                        "CREATE TABLE t WITH ('properties.password' = 'secret', 'properties.username' = 'pass')");
        List<RowData> actual = collector.collect(result);

        assertThat(actual.get(0).getString(0).toString())
                .isEqualTo(
                        "CREATE TABLE t WITH ('properties.password' = '****', 'properties.username' = 'pass')");
    }

    @Test
    void testFailsForMultipleColumns() {
        var collector = new ShowCreateResultCollector(true, new String[] {"password"});
        var schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .build()
                        .resolve(new TestSchemaResolver());
        var result = new ShowCreateTableResult("dummy", schema);

        assertThatThrownBy(() -> collector.collect(result))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not supported for multiple columns");
    }

    @Test
    void testFailsForNonTextColumn() {
        var collector = new ShowCreateResultCollector(true, new String[] {"password"});

        var schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.BIGINT())
                        .build()
                        .resolve(new TestSchemaResolver());
        var result = new ShowCreateTableResult("dummy", schema);

        assertThatThrownBy(() -> collector.collect(result))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Operation supported only for text column");
    }

    private static TableResultInternal createShowCreateTableResult(String ddl) {
        return new ShowCreateTableResult(ddl);
    }

    private static class ShowCreateTableResult implements TableResultInternal {

        private final String ddl;
        private final ResolvedSchema schema;

        private ShowCreateTableResult(String ddl) {
            this(ddl, createSchema());
        }

        public ShowCreateTableResult(String ddl, ResolvedSchema schema) {
            this.ddl = ddl;
            this.schema = schema;
        }

        private static ResolvedSchema createSchema() {
            return Schema.newBuilder()
                    .column("f0", DataTypes.STRING())
                    .build()
                    .resolve(new TestSchemaResolver());
        }

        @Override
        public CloseableIterator<RowData> collectInternal() {
            RowData row = GenericRowData.of(StringData.fromString(ddl));
            return CloseableIterator.adapterForIterator(List.of(row).iterator());
        }

        @Override
        public RowDataToStringConverter getRowDataToStringConverter() {
            return null;
        }

        @Override
        public CachedPlan getCachedPlan() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<JobClient> getJobClient() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void await() {}

        @Override
        public void await(long l, TimeUnit timeUnit) {}

        @Override
        public ResolvedSchema getResolvedSchema() {
            return schema;
        }

        @Override
        public ResultKind getResultKind() {
            return ResultKind.SUCCESS_WITH_CONTENT;
        }

        @Override
        public CloseableIterator<Row> collect() {
            var row = Row.withPositions(1);
            row.setField(1, ddl);
            return CloseableIterator.adapterForIterator(List.of(row).iterator());
        }

        @Override
        public void print() {
            throw new UnsupportedOperationException();
        }
    }
}
