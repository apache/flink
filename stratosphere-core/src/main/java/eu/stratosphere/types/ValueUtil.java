package eu.stratosphere.types;

/**
 * convert the java.lang type into stratosphere type
 */
public class ValueUtil {

	public static Value toStratosphere(Object java)  {
		
		if (java instanceof java.lang.Boolean) 
			return new BooleanValue(((java.lang.Boolean)java).booleanValue());
		if (java instanceof java.lang.Integer)
			return new IntValue(((java.lang.Integer)java).intValue());
		if (java instanceof java.lang.Byte)
			return new ByteValue(((java.lang.Byte)java).byteValue());
		if (java instanceof java.lang.Character)
			return new CharValue(((java.lang.Character)java).charValue());
		if (java instanceof java.lang.Double)
			return new DoubleValue(((java.lang.Double)java).doubleValue());
		if (java instanceof java.lang.Float)
			return new FloatValue(((java.lang.Float)java).floatValue());
		if (java instanceof java.lang.Long)
			return new LongValue(((java.lang.Long)java).longValue());
		if (java instanceof java.lang.Short)
			return new ShortValue(((java.lang.Short)java).shortValue());
		if (java instanceof java.lang.String)
			return new StringValue(((java.lang.String)java).toString());
		if (java == null)
		    return NullValue.getInstance();
		throw new IllegalArgumentException("unsupported java value");
		
	}
}
