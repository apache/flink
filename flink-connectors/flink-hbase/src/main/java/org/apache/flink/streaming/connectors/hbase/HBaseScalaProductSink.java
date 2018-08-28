package org.apache.flink.streaming.connectors.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import scala.Product;

/**
 * Sink to write scala tuples and case classes into a HBase cluster.
 *
 * @param <IN> Type of the elements emitted by this sink, it must extend {@link Product}
 */
public class HBaseScalaProductSink<IN extends Product> extends HBaseSinkBase<IN> {

	private final HBaseTableMapper tableMapper;
	private final String[] indexList;

	public HBaseScalaProductSink(HBaseTableBuilder builder, HBaseTableMapper tableMapper) {
		super(builder);
		this.tableMapper = tableMapper;
		this.indexList = tableMapper.getKeyList();
	}

	@VisibleForTesting
	public HBaseScalaProductSink(Table hTable, HBaseTableMapper tableMapper) {
		super(hTable);
		this.tableMapper = tableMapper;
		this.indexList = tableMapper.getKeyList();
	}

	@Override protected Object extract(IN value) throws Exception {
		int rowKeyIndex = Integer.parseInt(tableMapper.getRowKey());
		byte[] rowKey = HBaseTableMapper.serialize(tableMapper.getRowKeyType(), value.productElement(rowKeyIndex));
		Put put = new Put(rowKey);
		for (String index : indexList) {
			Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(index);
			put.addColumn(colInfo.f0, colInfo.f1,
				HBaseTableMapper.serialize(colInfo.f2, value.productElement(Integer.parseInt(index))));
		}
		return put;
	}
}
