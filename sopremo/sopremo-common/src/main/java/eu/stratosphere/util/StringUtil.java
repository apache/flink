package eu.stratosphere.util;

public class StringUtil {

	public static String camelCase(String input) {
		char[] chars = input.toCharArray();

		boolean capitalize = true;
		for (int index = 0; index < chars.length; index++)
			if (Character.isWhitespace(chars[index]))
				capitalize = true;
			else if (capitalize) {
				chars[index] = Character.toUpperCase(chars[index]);
				capitalize = false;
			} else
				chars[index] = Character.toLowerCase(chars[index]);

		return new String(chars);
	}

	public static String lowerFirstChar(String input) {
		if (input.isEmpty())
			return input;
		
		char[] chars = input.toCharArray();
		chars[0] = Character.toLowerCase(chars[0]);
		return new String(chars);
	}
	
	public static String upperFirstChar(String input) {
		if (input.isEmpty())
			return input;
		
		char[] chars = input.toCharArray();
		chars[0] = Character.toUpperCase(chars[0]);
		return new String(chars);
	}

}
