package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * Flink Sink to save Row into a HBase cluster.
 */
public class HBaseRowSink extends HBaseSinkBase<Row> {
	private static final long serialVersionUID = 1L;
	private final HBaseTableMapper tableMapper;
	private final RowTypeInfo typeInfo;
	private final String[] fieldNameList;
	private int rowKeyFieldIndex;

	public HBaseRowSink(HBaseTableBuilder builder, HBaseTableMapper tableMapper, RowTypeInfo typeInfo) {
		super(builder);
		this.tableMapper = tableMapper;
		this.typeInfo = typeInfo;
		this.fieldNameList = tableMapper.getKeyList();
	}

	@VisibleForTesting
	public HBaseRowSink(Table hTable, HBaseTableMapper tableMapper, RowTypeInfo typeInfo) {
		super(hTable);
		this.tableMapper = tableMapper;
		this.typeInfo = typeInfo;
		this.fieldNameList = tableMapper.getKeyList();
	}

	@Override protected Object extract(Row value) throws Exception {
		byte[] rowKey = HBaseTableMapper.serialize(typeInfo.getTypeAt(rowKeyFieldIndex), value.getField(rowKeyFieldIndex));
		Put put = new Put(rowKey);
		for (int i = 0; i < fieldNameList.length; i++) {
			Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(fieldNameList[i]);
			put.addColumn(colInfo.f0, colInfo.f1,
				HBaseTableMapper.serialize(colInfo.f2, value.getField(typeInfo.getFieldIndex(fieldNameList[i]))));
		}
		return put;
	}

	@Override public void open(Configuration configuration) throws Exception {
		super.open(configuration);
		rowKeyFieldIndex = typeInfo.getFieldIndex(tableMapper.getRowKey());
	}
}
