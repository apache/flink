package org.apache.flink.mongodb.table.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.mongodb.table.sink.MongodbSinkConf;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;

public class MongodbDynamicTableSource implements ScanTableSource {
    private final MongodbSinkConf mongodbSinkConf;
    private final TableSchema tableSchema;

    /**
     * Watermark strategy that is used to generate per-partition watermark.
     */
    private WatermarkStrategy<RowData> watermarkStrategy;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public MongodbDynamicTableSource(MongodbSinkConf mongodbSinkConf, TableSchema tableSchema, DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.mongodbSinkConf = mongodbSinkConf;
        this.tableSchema = tableSchema;
        this.watermarkStrategy = null;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public DynamicTableSource copy() {
        return new MongodbDynamicTableSource(this.mongodbSinkConf, this.tableSchema, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(context, tableSchema.toRowDataType());

        return new DataStreamScanProvider() {
            @Override
            public boolean isBounded() {
                return true;
            }

            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                if (watermarkStrategy == null) {
                    watermarkStrategy = WatermarkStrategy.noWatermarks();
                }

                return execEnv.addSource(new MongodbBaseSourceFunction(mongodbSinkConf, deserializer)).returns(RowData.class);
            }
        };
    }


}
