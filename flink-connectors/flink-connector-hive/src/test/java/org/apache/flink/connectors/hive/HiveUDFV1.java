package org.apache.flink.connectors.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

@UDFType(deterministic = false)
public class HiveUDFV1 extends UDF {
	public boolean evaluate(String content) {
		if (StringUtils.isEmpty(content)) {
			return false;
		}
		else {
			return true;
		}
	}
}
