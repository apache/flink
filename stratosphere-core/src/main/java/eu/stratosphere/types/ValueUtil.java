package eu.stratosphere.types;

/**
 * convert the java.lang type into Stratosphere type
 */
public class ValueUtil {
	public static Value toStratosphere(Object java)  {
		if (java == null) {
		    return NullValue.getInstance();
        }
		if (java.getClass() == java.lang.Boolean.class) {
			return new BooleanValue(((java.lang.Boolean)java).booleanValue());
        }
		if (java.getClass() == java.lang.Integer.class) {
			return new IntValue(((java.lang.Integer)java).intValue());
        }
		if (java.getClass() == java.lang.Byte.class) {
			return new ByteValue(((java.lang.Byte)java).byteValue());
        }
		if (java.getClass() == java.lang.Character.class) {
			return new CharValue(((java.lang.Character)java).charValue());
        }
		if (java.getClass() == java.lang.Double.class) {
			return new DoubleValue(((java.lang.Double)java).doubleValue());
        }
		if (java.getClass() == java.lang.Float.class) {
			return new FloatValue(((java.lang.Float)java).floatValue());
        }
		if (java.getClass() == java.lang.Long.class) {
			return new LongValue(((java.lang.Long)java).longValue());
        }
		if (java.getClass() == java.lang.Short.class) {
			return new ShortValue(((java.lang.Short)java).shortValue());
        }
		if (java.getClass() == java.lang.String.class) {
			return new StringValue(((java.lang.String)java).toString());
        }
		throw new IllegalArgumentException("unsupported Java value");
	}
}
