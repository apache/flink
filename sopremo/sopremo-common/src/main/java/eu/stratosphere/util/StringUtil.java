package eu.stratosphere.util;

/**
 * Provides some convenience methods to transform Strings.
 */
public class StringUtil {

	/**
	 * Transforms a String into a camel case representation. Each whitespace is removed and the subsequent character is
	 * transformed to upper case.
	 * 
	 * @param input
	 *        the String that should be transformed
	 * @return the camel case representation of the given String
	 */
	public static String camelCase(final String input) {
		final char[] chars = input.toCharArray();

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

	/**
	 * This method transforms the first character of the given String to lower case.
	 * 
	 * @param input
	 *        the String that should be used
	 * @return the transformed String
	 */
	public static String lowerFirstChar(final String input) {
		if (input.isEmpty())
			return input;

		final char[] chars = input.toCharArray();
		chars[0] = Character.toLowerCase(chars[0]);
		return new String(chars);
	}

	/**
	 * This method transforms the first character of the given String to upper case.
	 * 
	 * @param input
	 *        the String that should be used
	 * @return the transformed String
	 */
	public static String upperFirstChar(final String input) {
		if (input.isEmpty())
			return input;

		final char[] chars = input.toCharArray();
		chars[0] = Character.toUpperCase(chars[0]);
		return new String(chars);
	}

}
