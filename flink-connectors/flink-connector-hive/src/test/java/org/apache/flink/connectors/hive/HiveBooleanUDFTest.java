package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class HiveBooleanUDFTest extends UDF {
	public Boolean evaluate(int content) {
		if (content == 1) {
			return true;
		} else {
			return false;
		}
	}
}
