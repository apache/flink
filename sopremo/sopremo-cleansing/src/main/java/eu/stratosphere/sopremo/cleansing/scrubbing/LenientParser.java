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

	private final Map<Class<? extends JsonNode>, List<Parser>> parsers = new IdentityHashMap<Class<? extends JsonNode>, List<Parser>>();

	private final static Comparator<Parser> ParserComparator = new Comparator<Parser>() {
		@Override
		public int compare(final Parser o1, final Parser o2) {
			return o1.parseLevel - o2.parseLevel;
		}
	};

	private LenientParser() {
		this.addBooleanParsers();
	}

	public void add(final Class<? extends JsonNode> type, final Parser parser) {
		List<Parser> parserList = this.parsers.get(type);
		if (parserList == null)
			this.parsers.put(type, parserList = new ArrayList<Parser>());
		final int pos = Collections.binarySearch(parserList, parser, ParserComparator);
		if (pos < 0)
			parserList.add(-pos - 1, parser);
		else
			parserList.set(pos, parser);
	}

	protected void addBooleanParsers() {
		this.add(BooleanNode.class, new Parser(STRICT) {
			@Override
			public JsonNode parse(final String textualValue, final int parseLevel) {
				if (textualValue.equalsIgnoreCase("true"))
					return BooleanNode.TRUE;
				if (textualValue.equalsIgnoreCase("false"))
					return BooleanNode.FALSE;
				return null;
			}
		});
		this.add(BooleanNode.class, new Parser(ELIMINATE_NOISE) {
			Pattern truePattern = Pattern.compile("(?i).*t.*r.*u*.e.*");

			Pattern falsePattern = Pattern.compile("(?i).*f.*a.*l*.s*.e.*");

			@Override
			public JsonNode parse(final String textualValue, final int parseLevel) {
				if (this.truePattern.matcher(textualValue).matches())
					return BooleanNode.TRUE;
				if (this.falsePattern.matcher(textualValue).matches())
					return BooleanNode.FALSE;
				return null;
			}
		});
		this.add(BooleanNode.class, new Parser(FORCE_PARSING) {
			@Override
			public JsonNode parse(final String textualValue, final int parseLevel) {
				return BooleanNode.FALSE;
			}
		});
	}

	public JsonNode parse(final String value, final Class<? extends JsonNode> type, final int parseLevel) {
		final List<Parser> parserList = this.parsers.get(type);
		if (parserList == null)
			throw new ParseException("no parser for value type " + type);
		for (final Parser parser : parserList) {
			if (parser.parseLevel > parseLevel)
				break;
			final JsonNode result = parser.parse(value, parseLevel);
			if (result != null)
				return result;
		}
		throw new ParseException(String.format("cannot parse value %s to type %s with parser level %s", value, type,
			parseLevel));
	}

	public static abstract class Parser {
		private final int parseLevel;

		public Parser(final int parseLevel) {
			this.parseLevel = parseLevel;
		}

		public abstract JsonNode parse(String textualValue, int parseLevel);
	}
}
