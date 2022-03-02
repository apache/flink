package org.apache.flink.connector.pulsar.table;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PulsarTableTestUtils {
    public static List<Row> collectRows(Table table, int expectedSize) throws Exception {
        final TableResult result = table.execute();
        final List<Row> collectedRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (collectedRows.size() < expectedSize && iterator.hasNext()) {
                collectedRows.add(iterator.next());
            }
        }
        result.getJobClient()
                .ifPresent(
                        jc -> {
                            try {
                                jc.cancel().get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        return collectedRows;
    }
}
