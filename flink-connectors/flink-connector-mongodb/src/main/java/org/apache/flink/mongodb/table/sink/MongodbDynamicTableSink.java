package org.apache.flink.mongodb.table.sink;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

public class MongodbDynamicTableSink implements DynamicTableSink {
    private final MongodbSinkConf mongodbSinkConf;
    private final ResolvedSchema tableSchema;

    public MongodbDynamicTableSink(MongodbSinkConf mongodbSinkConf, ResolvedSchema tableSchema) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType dataType = tableSchema.toSinkRowDataType();
        DataStructureConverter converter = context.createDataStructureConverter(dataType);
        return SinkFunctionProvider.of(new MongodbUpsertSinkFunction(
                this.mongodbSinkConf,
                tableSchema.getColumnNames(),
                converter));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(this.mongodbSinkConf, this.tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }
}
