package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.typeinfo.OutputTag;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;

import java.util.Arrays;
import java.util.List;


/**
 * Created by chenqin on 10/21/16.
 */
public class OutputTagUtil {
	public static List<String> getOutputTagName(OutputTag tag){
		return Arrays.asList(tag.getTypeInfo().toString());
	}

	public static TypeInformation getSideOutputTypeInfo(List<String> names){
		//HACK Alert
		if(names.size() == 1 && names.get(0) == "String") {
			return TypeInfoParser.parse(names.get(0));
		} else {
			return null;
		}
	}
}
