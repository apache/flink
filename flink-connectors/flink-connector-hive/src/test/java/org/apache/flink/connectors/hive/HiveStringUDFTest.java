package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveStringUDFTest extends UDF {
	public String evaluate(String content) {
		return content;
	}
}
