package eu.stratosphere.pact.compiler.properties;


/**
 *
 *
 * 
 */
public abstract class Property<T extends Property<T>>
{
	protected final int[] fields;
	
	
	protected Property(int[] fields)
	{
		if (fields == null)
			throw new NullPointerException();
		
		this.fields = fields;
	}
	
	
	public int[] getFields()
	{
		return this.fields;
	}
	
	public boolean areTheseFieldsPrefix(int[] otherFields)
	{
		if (otherFields == null || otherFields.length < this.fields.length) {
			return false;
		}
		
		for (int i = 0; i < this.fields.length; i++) {
			if (this.fields[i] != otherFields[i])
				return false;
		}
		
		return true;
	}
	
	public boolean areOtherFieldsPrefix(int[] otherFields)
	{
		if (otherFields == null || otherFields.length > this.fields.length) {
			return false;
		}
		
		for (int i = 0; i < otherFields.length; i++) {
			if (this.fields[i] != otherFields[i])
				return false;
		}
		
		return true;
	}
	
	
	public abstract String getName();
	
	public abstract boolean satisfiesRequiredProperty(T requiredProperty);
	
	public abstract boolean isTrivial();
	
}
