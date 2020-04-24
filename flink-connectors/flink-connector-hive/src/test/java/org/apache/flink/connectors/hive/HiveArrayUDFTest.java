package org.apache.flink.connectors.hive;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class HiveArrayUDFTest extends UDF {
	public List<String> evaluate(String content) {
		List<String> list = new ArrayList<>();
		list.add(content);
		return list;
	}
}
