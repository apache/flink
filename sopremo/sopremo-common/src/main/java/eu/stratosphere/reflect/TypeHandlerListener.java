package eu.stratosphere.reflect;

public interface TypeHandlerListener<InputType, OutputType> {
	public void beforeConversion(InputType in, Object[] params);
	public void afterConversion(InputType in, Object[] params, OutputType out);
}
