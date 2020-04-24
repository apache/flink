package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveLongUDFTest extends UDF {
	public Long evaluate(Long content) {
		return content;
	}
}
