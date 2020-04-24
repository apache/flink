package org.apache.flink.connectors.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class HiveIntergerUDFTest extends UDF {
	public Integer evaluate(int content) {
		return content;
	}
}
