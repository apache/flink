package org.apache.flink.mongodb.table.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

public class MongodbDynamicTableSink implements DynamicTableSink {
    private final MongodbSinkConf mongodbSinkConf;
    private final TableSchema tableSchema;

    public MongodbDynamicTableSink(MongodbSinkConf mongodbSinkConf, TableSchema tableSchema) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        // I、-U、+U、D
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        // 初始化数据结构转换器，可以将二进制的数据转换成flink可操作的Row
        DataStructureConverter converter = context.createDataStructureConverter(this.tableSchema.toRowDataType());
        return SinkFunctionProvider.of(new MongodbUpsertSinkFunction(this.mongodbSinkConf, this.tableSchema.getFieldNames(), converter));
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
