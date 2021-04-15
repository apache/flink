package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.postgresql.xa.PGXADataSource;

public class JdbcXaSinkFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

        // JDBC Sink
        //        env.addSource(new CounterSource()).setParallelism(2)
        //            .addSink(
        //                    JdbcSink.sink(
        //                        "insert into book (id, title, authors, year) values (?, ?, ?, ?)",
        //                        (statement, book) -> {
        //                            statement.setInt(1, book.id);
        //                            statement.setString(2, book.title);
        //                            statement.setString(3, book.authors);
        //                            statement.setInt(4, book.year);
        //                        },
        //                        JdbcExecutionOptions.builder()
        //                                .withBatchSize(1000)
        //                                .withBatchIntervalMs(200)
        //                                .withMaxRetries(5)
        //                                .build(),
        //                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        //                                .withUrl("jdbc:postgresql://localhost:5432/postgres")
        //                                .withDriverName("org.postgresql.Driver")
        //                                .build()
        //                ));

        // JDBC Exactly-once Sink
        env.addSource(new CounterSource())
                .setParallelism(2)
                .addSink(
                        JdbcSink.exactlyOnceSink(
                                "insert into book (id, title, authors, year) values (?, ?, ?, ?)",
                                (statement, book) -> {
                                    statement.setInt(1, book.id);
                                    statement.setString(2, book.title);
                                    statement.setString(3, book.authors);
                                    statement.setInt(4, book.year);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:postgresql://localhost:5432/postgres")
                                        .withDriverName("org.postgresql.Driver")
                                        .build(),
                                JdbcExactlyOnceOptions.defaults(),
                                () -> {
                                    // create a driver-specific XA DataSource
                                    PGXADataSource pgxaDataSource = new PGXADataSource();
                                    pgxaDataSource.setUrl(
                                            "jdbc:postgresql://localhost:5432/postgres");
                                    //
                                    // pgxaDataSource.setServerName("localhost");
                                    //                        int[] port = {5432};
                                    //                        pgxaDataSource.setPortNumbers(port);
                                    //
                                    // pgxaDataSource.setDatabaseName("postgres");

                                    return pgxaDataSource;
                                }));

        env.execute();
    }

    static class Book {
        public Book(Integer id, String title, String authors, Integer year) {
            this.id = id;
            this.title = title;
            this.authors = authors;
            this.year = year;
        }

        final Integer id;
        final String title;
        final String authors;
        final Integer year;
    }

    static class CounterSource extends RichParallelSourceFunction<Book>
            implements CheckpointedFunction {

        private Integer offset = 0;

        private ListState<Integer> state;

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Book> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();

            while (isRunning && offset < 100) {
                synchronized (lock) {
                    if (offset % 5 == 0) {
                        Thread.sleep(100);
                    }

                    ctx.collect(
                            new Book(offset, "name" + offset, "author" + offset, 1000 + offset));
                    offset += 1;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>("state", IntSerializer.INSTANCE));

            // restore any state that we might already have to our fields, initialize state
            // is also called in case of restore.
            for (Integer i : state.get()) {
                offset = i;
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.clear();
            state.add(offset);
        }
    }
}
