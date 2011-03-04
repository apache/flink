import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import sun.reflect.generics.tree.Tree;
import antlr.RecognitionException;

import com.ibm.jaql.lang.Jaql;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

import eu.stratosphere.simple.jaql.rewrite.RewriteEngine;
import eu.stratosphere.sopremo.JaqlPlanCreator;

public class ParserTest {
	public static void main(String[] args) throws Exception {

		// CommonTokenStream tokens = lex(new FileInputStream("test.jaql"));
		//
		// CommonTree tree = parse(tokens);
		// CommonTreeNodeStream nodes = new CommonTreeNodeStream(tree);
		// System.out.println("parsed tree: " + toStringTree(tree));
		//
		// JaqlTree walker = new JaqlTree(nodes);
		// tree = (CommonTree) walker.downup(tree, true); // walk t, trace transforms
		// System.out.println("transformed tree: " + toStringTree(tree));
//
//		Jaql jaql = new Jaql() {
//			{
//				rewriter = new RewriteEngine();
//			}
//		};
//		//jaql.enableRewrite(false);
//		jaql.setInput("test", new FileInputStream("test.jaql"));
////		jaql.setInput("test", new FileInputStream("scrub.jaql"));
//		Expr expr = jaql.expr();
//		System.out.println(expr);

		System.out.println(new JaqlPlanCreator().getPlan(new FileInputStream("test.jaql")));
		
		System.exit(0);

	}
	//
	// private static String toStringTree(CommonTree tree) {
	// StringBuilder builder = new StringBuilder();
	// appendSubTree(builder, tree, 1);
	// return builder.toString();
	// }
	//
	// private static void appendSubTree(StringBuilder builder, Tree tree, int indentation) {
	// for (int index = 0; index < 2 * indentation; index++)
	// builder.append(' ');
	// builder.append(tree);
	// boolean onlyLeaves = true;
	// for (int index = 0, count = tree.getChildCount(); index < count; index++)
	// onlyLeaves &= tree.getChild(index).getChildCount() == 0;
	//
	// if (onlyLeaves) {
	// for (int index = 0, count = tree.getChildCount(); index < count; index++) {
	// builder.append(' ');
	// builder.append(tree.getChild(index));
	// }
	// builder.append('\n');
	// } else {
	// builder.append('\n');
	// for (int index = 0, count = tree.getChildCount(); index < count; index++)
	// appendSubTree(builder, tree.getChild(index), indentation + 1);
	// }
	// }
	//
	// private static CommonTree parse(CommonTokenStream tokens) throws RecognitionException {
	// JaqlParser parser = new JaqlParser(tokens);
	// JaqlParser.script_return example = parser.script();
	//
	// CommonTree tree = (CommonTree) example.getTree();
	// return tree;
	// }
	//
	// private static CommonTokenStream lex(InputStream inputStream) throws IOException {
	// CommonTokenStream tokens = new CommonTokenStream();
	// ANTLRInputStream input = new ANTLRInputStream(inputStream);
	// JaqlLexer lexer = new JaqlLexer(input);
	// tokens.setTokenSource(lexer);
	// return tokens;
	// }

}
