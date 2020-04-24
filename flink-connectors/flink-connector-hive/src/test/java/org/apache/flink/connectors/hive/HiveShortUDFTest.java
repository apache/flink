package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveShortUDFTest extends UDF {
	public Short evaluate(Short content) {
		return content;
	}
}
