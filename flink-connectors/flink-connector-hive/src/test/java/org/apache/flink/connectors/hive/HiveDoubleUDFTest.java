package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveDoubleUDFTest extends UDF {
	public Double evaluate(Double content) {
		return content;
	}
}
