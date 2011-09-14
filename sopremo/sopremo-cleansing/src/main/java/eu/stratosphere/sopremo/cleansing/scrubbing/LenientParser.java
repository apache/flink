package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.TypeCoercer;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
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

	private final static Comparator<Parser> ParserComparator = new Comparator<Parser>() {
		@Override
		public int compare(final Parser o1, final Parser o2) {
			return o1.parseLevel - o2.parseLevel;
		}
	};

	/**
	 * The default instance.
	 */
	public final static LenientParser INSTANCE = new LenientParser();

	private final Map<Class<? extends JsonNode>, List<Parser>> parsers = new IdentityHashMap<Class<? extends JsonNode>, List<Parser>>();

	private LenientParser() {
		this.addBooleanParsers();
		this.addNumberParsers();
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

	private void addNumberParsers() {
		final Pattern removeNonInt = Pattern.compile("[^0-9+-]");
		final Pattern removeNonFloat = Pattern.compile("[^0-9+-.]");
		for (final Class<? extends NumericNode> type : TypeCoercer.NUMERIC_TYPES) {
			this.add(type, new Parser(STRICT) {
				@Override
				public JsonNode parse(TextNode textNode) {
					return TypeCoercer.INSTANCE.coerce(textNode, type);
				}
			});
			final NumericNode defaultValue = TypeCoercer.INSTANCE.coerce(TextNode.valueOf("0"), type);
			this.add(type, new TextualParser(ELIMINATE_NOISE) {
				Pattern removePattern = defaultValue.isIntegralNumber() ? removeNonInt : removeNonFloat;

				@Override
				public JsonNode parse(final String textualValue) {
					StringBuilder cleanedValue = new StringBuilder(this.removePattern.matcher(textualValue).replaceAll(
						""));
					if (!this.removeSuperfluxSigns(cleanedValue))
						return null;

					if (defaultValue.isFloatingPointNumber())
						this.remomveSuperfluxDots(cleanedValue);

					return TypeCoercer.INSTANCE.coerce(TextNode.valueOf(cleanedValue.toString()), type);
				}

				private void remomveSuperfluxDots(StringBuilder cleanedValue) {
					int lastDotIndex = cleanedValue.lastIndexOf(".");
					while (lastDotIndex > 0 && (lastDotIndex = cleanedValue.lastIndexOf(".", lastDotIndex - 1)) != -1)
						cleanedValue.deleteCharAt(lastDotIndex);
				}

				private boolean removeSuperfluxSigns(StringBuilder cleanedValue) {
					int numberStart = -1;
					for (int index = 0; index < cleanedValue.length(); index++)
						if (Character.isDigit(cleanedValue.charAt(index))) {
							numberStart = index;
							break;
						}
					if (numberStart == -1)
						return false;
					if (numberStart > 1)
						cleanedValue.delete(0, numberStart - 1);
					return true;
				}
			});
			this.add(type, new TextualParser(FORCE_PARSING) {

				@Override
				public JsonNode parse(final String textualValue) {
					return defaultValue;
				}
			});
		}
	}

	private void addBooleanParsers() {
		this.add(BooleanNode.class, new TextualParser(STRICT) {
			@Override
			public JsonNode parse(final String textualValue) {
				if (textualValue.equalsIgnoreCase("true"))
					return BooleanNode.TRUE;
				if (textualValue.equalsIgnoreCase("false"))
					return BooleanNode.FALSE;
				return null;
			}
		});
		this.add(BooleanNode.class, new TextualParser(ELIMINATE_NOISE) {
			Pattern truePattern = Pattern.compile("(?i).*t.*r.*u*.e.*");

			Pattern falsePattern = Pattern.compile("(?i).*f.*a.*l*.s*.e.*");

			@Override
			public JsonNode parse(final String textualValue) {
				if (this.truePattern.matcher(textualValue).matches())
					return BooleanNode.TRUE;
				if (this.falsePattern.matcher(textualValue).matches())
					return BooleanNode.FALSE;
				return null;
			}
		});
		this.add(BooleanNode.class, new TextualParser(FORCE_PARSING) {
			@Override
			public JsonNode parse(final String textualValue) {
				return BooleanNode.FALSE;
			}
		});
	}

	public JsonNode parse(final TextNode value, final Class<? extends JsonNode> type, final int parseLevel) {
		List<Parser> parserList = this.parsers.get(type);
		if (parserList == null)
			throw new ParseException("no parser for value type " + type);
		for (final Parser parser : parserList) {
			if (parser.parseLevel > parseLevel)
				break;
			final JsonNode result = parser.parse(value);
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

		public int getParseLevel() {
			return this.parseLevel;
		}

		public abstract JsonNode parse(TextNode textNode);
	}

	public static abstract class TextualParser extends Parser {

		public TextualParser(int parseLevel) {
			super(parseLevel);
		}

		public abstract JsonNode parse(String textualValue);

		@Override
		public final JsonNode parse(TextNode textNode) {
			return this.parse(textNode.getTextValue());
		}
	}
}
