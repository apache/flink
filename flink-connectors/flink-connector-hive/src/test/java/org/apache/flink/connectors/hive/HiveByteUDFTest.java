package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveByteUDFTest extends UDF {
	public Byte evaluate(Byte content) {
		return content;
	}
}
