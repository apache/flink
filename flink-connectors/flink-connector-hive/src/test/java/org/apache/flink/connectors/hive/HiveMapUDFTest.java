package org.apache.flink.connectors.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;

import java.util.HashMap;
import java.util.Map;

@UDFType(deterministic = false)
public class HiveMapUDFTest extends UDF {
	public HashMap<String, String> evaluate(String key, String value) {
		HashMap<String, String> testMap = new HashMap<>();
		testMap.put(key, value);
		testMap.put("1231","234");
		return testMap;
	}
}
