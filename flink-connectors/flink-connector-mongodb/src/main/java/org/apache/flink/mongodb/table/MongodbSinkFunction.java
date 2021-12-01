package org.apache.flink.mongodb.table;

import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** */
public class MongodbSinkFunction extends MongodbBaseSinkFunction<RowData> {
    private final DataStructureConverter converter;
    private final DataType physicalRowDataType;

    public MongodbSinkFunction(
            MongodbSinkConf mongodbSinkConf,
            DynamicTableSink.DataStructureConverter converter,
            DataType physicalRowDataType) {
        super(mongodbSinkConf);
        this.converter = converter;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    Document invokeDocument(RowData value, Context context) {
        List<String> fieldNames = DataType.getFieldNames(physicalRowDataType);
        Row row = (Row) this.converter.toExternal(value);
        Map<String, Object> map = new HashMap();
        for (int i = 0; i < fieldNames.size(); i++) {
            map.put(fieldNames.get(i), row.getField(i));
        }
        return new Document(map);
    }
}
