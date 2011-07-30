package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

public class LenientParser {
	/**
	 * Fail if no exact parse is possible.
	 */
	public final static int STRICT = 0;

	/**
	 * Remove letters for numbers, ...
	 */
	public final static int ELIMINATE_NOISE = 100;

	/**
	 * Try to unveil values that are somehow encoded, e.g., numbers as texts.
	 */
	public final static int INTERPRET_VALUES = 200;

	/**
	 * Try everything and return default value if nothing succeeded. Never fails.
	 */
	public final static int FORCE_PARSING = 300;

	/**
	 * The default instance.
	 */
	public final static LenientParser INSTANCE = new LenientParser();

	public JsonNode parse(String value, Class<? extends JsonNode> type, int parseLevel) {
		List<Parser> parserList = parsers.get(type);
		if (parserList == null)
			throw new ParseException("no parser for value type " + type);
		for (Parser parser : parserList) {
			if (parser.parseLevel > parseLevel)
				break;
			JsonNode result = parser.parse(value, parseLevel);
			if (result != null)
				return result;
		}
		throw new ParseException(String.format("cannot parse value %s to type %s with parser level %s", value, type,
			parseLevel));
	}

	private Map<Class<? extends JsonNode>, List<Parser>> parsers = new IdentityHashMap<Class<? extends JsonNode>, List<Parser>>();

	private LenientParser() {
		addBooleanParsers();
	}

	protected void addBooleanParsers() {
		add(BooleanNode.class, new Parser(STRICT) {
			@Override
			public JsonNode parse(String textualValue, int parseLevel) {
				if (textualValue.equalsIgnoreCase("true"))
					return BooleanNode.TRUE;
				if (textualValue.equalsIgnoreCase("false"))
					return BooleanNode.FALSE;
				return null;
			}
		});
		add(BooleanNode.class, new Parser(ELIMINATE_NOISE) {
			Pattern truePattern = Pattern.compile("(?i).*t.*r.*u*.e.*");
			Pattern falsePattern = Pattern.compile("(?i).*f.*a.*l*.s*.e.*");
			
			@Override
			public JsonNode parse(String textualValue, int parseLevel) {
				if (truePattern.matcher(textualValue).matches())
					return BooleanNode.TRUE;
				if (falsePattern.matcher(textualValue).matches())
					return BooleanNode.FALSE;
				return null;
			}
		});
		add(BooleanNode.class, new Parser(FORCE_PARSING) {
			@Override
			public JsonNode parse(String textualValue, int parseLevel) {
				return BooleanNode.FALSE;
			}
		});
	}

	public void add(Class<? extends JsonNode> type, Parser parser) {
		List<Parser> parserList = parsers.get(type);
		if (parserList == null)
			parsers.put(type, parserList = new ArrayList<Parser>());
		int pos = Collections.binarySearch(parserList, parser, ParserComparator);
		if (pos < 0)
			parserList.add(-pos - 1, parser);
		else
			parserList.set(pos, parser);
	}

	private final static Comparator<Parser> ParserComparator = new Comparator<Parser>() {
		@Override
		public int compare(Parser o1, Parser o2) {
			return o1.parseLevel - o2.parseLevel;
		}
	};

	public static abstract class Parser {
		private int parseLevel;

		public Parser(int parseLevel) {
			this.parseLevel = parseLevel;
		}

		public abstract JsonNode parse(String textualValue, int parseLevel);
	}
}
