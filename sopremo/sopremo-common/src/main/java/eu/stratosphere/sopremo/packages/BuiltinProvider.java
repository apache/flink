package eu.stratosphere.sopremo.packages;

/**
 * Tag interface to indicate that the given class provides built-in functions and constants.<br>
 * There are two ways to specify functions that are automatically added to the {@link IFunctionRegistry}:
 * <ul>
 * <li>Implement a public, static function that only takes {@link eu.stratosphere.sopremo.type.IJsonNode}s or sub-types
 * as parameters and returns a IJsonNode or sub-types or void. The first parameter of such a function is the return
 * value.<br>
 * If the type of the return value of a function is fixed, then the return type of the function should be void:<br>
 * <code>public static void rand(DoubleNode result) {
 *   result.setValue(Math.random());
 * }</code><br>
 * However, in cases where the result is not known beforehand, the method returns an IJsonNode.<br>
 * <code>public static INumericNode add(INumericNode result, INumericNode first, INumericNode second) {
 *   result = ArithmeticOperator.ADDITION.evaluate(first, second, result);
 *   return result;
 * }</code><br>
 * The result parameter takes the result of the last invocation. Thus, on the first invocation the parameter is null if
 * the type is not directly instantiable.
 * <li>The second way is to specify constants that inherit directly or indirectly from
 * {@link eu.stratosphere.sopremo.function.AggregationFunction}. These functions will be transparently added as
 * {@link eu.stratosphere.sopremo.function.SopremoFunction}s to the registry. Please note, that only these methods can
 * be used in aggregating expressions.
 * </ul>
 * <br>
 * Additionally, a BuiltinProvider may define constants in the usual way. All public static final constants that are
 * {@link eu.stratosphere.sopremo.type.IJsonNode}s are automatically added to the {@link IConstantRegistry}. <br>
 * To exert greater control of the registration process, pleases refer to {@link FunctionRegistryCallback},
 * {@link ConstantRegistryCallback}.
 * 
 * @author Arvid Heise
 * @see FunctionRegistryCallback
 * @see ConstantRegistryCallback
 */
public interface BuiltinProvider {

}
