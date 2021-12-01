package org.apache.flink.mongodb.table;

import org.apache.flink.mongodb.internal.options.MongodbConnectorOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;

/** A {@link DynamicTableSink} for Mongodb. */
public class MongodbDynamicTableSink implements DynamicTableSink {
    private final MongodbSinkConf mongodbSinkConf;
    private final DataType physicalRowDataType;

    public MongodbDynamicTableSink(MongodbConnectorOptions options, DataType physicalRowDataType) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataStructureConverter converter =
                context.createDataStructureConverter(physicalRowDataType);

        return SinkFunctionProvider.of(
                new MongodbSinkFunction(this.mongodbSinkConf, converter, physicalRowDataType));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(this.mongodbSinkConf, this.physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }
}
