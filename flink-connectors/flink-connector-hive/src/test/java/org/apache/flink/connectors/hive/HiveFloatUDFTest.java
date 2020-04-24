package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveFloatUDFTest extends UDF {
	public Float evaluate(Float content) {
		return content;
	}
}
