package flink.graphs.example.utils;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.NullValue;

import flink.graphs.Edge;

public class EdgeWithLongIdNullValueParser extends RichMapFunction<String, Edge<Long, NullValue>> {
	private static final long serialVersionUID = 1L;

	public Edge<Long, NullValue> map(String value) {
		String[] nums = value.split(" ");
		return new Edge<Long, NullValue>(Long.parseLong(nums[0]), Long.parseLong(nums[1]), 
				NullValue.getInstance());
	}
}
