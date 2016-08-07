package com.fmaylinch.sqlmongo.parser;

import com.fmaylinch.sqlmongo.tokenizer.Token;
import com.fmaylinch.sqlmongo.tokenizer.Token.Type;
import com.fmaylinch.sqlmongo.tokenizer.Tokenizer;
import com.fmaylinch.sqlmongo.util.Fun;
import com.fmaylinch.sqlmongo.util.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.QueryOperators;
import org.bson.types.ObjectId;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SqlParser {

	private static final String ID = "_id";

	private static List<SimpleDateFormat> dateFormats =
			Arrays.asList(new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

	private final String querySql;
	private final DB db;

	/** Tokenizer used to parse the SQL query */
	private Tokenizer tokenizer;

	private ParseResult parseResult;


	public SqlParser(String querySql, DB db) {
		this.querySql = querySql.trim();
		this.db = db;
	}

	/**
	 * Parses the SQL query
	 */
	public ParseResult parse() {

		parseResult = new ParseResult();

		tokenizer = new Tokenizer(querySql);
		tokenizer.setKeywords(new HashSet<>(Arrays.asList(
				"select", "from", "where", "as", "and", "limit", "order", "by", "asc", "desc")));

		final BasicDBObject select = parseSelect();

		final DBCollection collection = parseFrom();

		final BasicDBObject query = parseWhere();

		parseResult.cursor = collection.find(query, select);

		if (isNextTokenSkipIt(Type.KEYWORD, "order")) {
			checkAndSkipNextToken(Type.KEYWORD, "by");
			parseOrders();
		}

		if (isNextTokenSkipIt(Type.KEYWORD, "limit")) {
			parseLimit();
		}

		return parseResult;
	}


	// Main parse groups

	private BasicDBObject parseSelect() {

		checkAndSkipNextToken(Type.KEYWORD, "select");

		BasicDBObject select = MongoUtil.obj();

		if (isNextTokenSkipIt(Type.SYMBOL, "*")) return select;

		do {
			parseSelectField(select);
		} while (isNextTokenSkipIt(Type.SYMBOL, ","));

		// Exclude ID if not selected (mongo includes ID by default)
		boolean excludeId = parseResult.fields.keySet().stream().noneMatch(f -> f.equals(ID));
		if (excludeId) {
			select.append(ID, 0);
		}

		return select;
	}

	private DBCollection parseFrom() {

		checkAndSkipNextToken(Type.KEYWORD, "from");

		String table = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		String alias = table;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		parseResult.tables.put(alias, table);
		return db.getCollection(table);
	}

	private BasicDBObject parseWhere()
	{
		BasicDBObject query = MongoUtil.obj();

		if (isNextTokenSkipIt(Type.KEYWORD, "where"))
		{
			do {
				Condition condition = parseCondition();
				query.append(condition.path, condition.getMongoValue());

			} while (isNextTokenSkipIt(Type.KEYWORD, "and"));
		}

		return query;
	}

	private void parseOrders()
	{
		BasicDBObject orders = MongoUtil.obj();

		do {

			String path = consumeNextPath();
			int direction = 1; // asc by default
			if (isNextTokenSkipIt(Type.KEYWORD, "asc")) {
				direction = 1;
			} else if (isNextTokenSkipIt(Type.KEYWORD, "desc")) {
				direction = -1;
			}

			orders.append(path, direction);

		} while (isNextTokenSkipIt(Type.SYMBOL, ","));

		parseResult.cursor.sort(orders);
	}

	private void parseLimit()
	{
		final Token numberToken = checkAndSkipNextToken(Type.NUMBER);
		parseResult.cursor.limit(Integer.parseInt(numberToken.getString()));
	}


	// Piece parsing

	private void parseSelectField(BasicDBObject select) {

		String path = consumeNextPath();
		String alias = path;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		select.append(path, 1);
		parseResult.fields.put(alias, path);
	}

	private Condition parseCondition()
	{
		Condition result = new Condition();

		result.path = consumeNextPath();
		result.operator = Operator.fromSqlOperator(checkAndSkipNextToken(Type.SYMBOL).getString());
		result.value = parseValue();

		return result;
	}

	private Object parseValue() {

		Token token = tokenizer.skipNextToken();

		switch (token.getType()) {
			case STRING: return token.getString();
			case NUMBER: return Double.parseDouble(token.getString());
			case BOOLEAN: return Boolean.parseBoolean(token.getString());
			case IDENTIFIER:
				switch (token.getString()) {
					case "Date": return parseDateArgument();
					case "Id": return parseIdArgument();
				}
		}

		throw new IllegalArgumentException("Unexpected value: " + token);
	}

	private Date parseDateArgument() {

		checkAndSkipNextToken(Type.SYMBOL, "(");
		String dateStr = consumeNextString();
		checkAndSkipNextToken(Type.SYMBOL, ")");

		try {
			for (SimpleDateFormat dateFormat : dateFormats) {
				if (dateStr.length() == dateFormat.toPattern().length()) {
					return dateFormat.parse(dateStr);
				}
			}
		} catch (ParseException e) {
			// We throw below (also if no suitable format is found)
		}

		throw new IllegalArgumentException("Unsupported date: " + dateStr
				+ " (available formats: " + Fun.map(dateFormats, f -> f.toPattern()) + ")");
	}

	private ObjectId parseIdArgument()
	{
		checkAndSkipNextToken(Type.SYMBOL, "(");
		String idStr = consumeNextString();
		checkAndSkipNextToken(Type.SYMBOL, ")");
		return new ObjectId(idStr);
	}


	// Tokenizer helper methods

	private String consumeNextString() {
		String stringWithQuotes = checkAndSkipNextToken(Type.STRING).getString();
		return stringWithQuotes.substring(1, stringWithQuotes.length()-1);
	}

	/** Joins next path made of IDENTIFIERs and dots like house.address.number */
	public String consumeNextPath()
	{
		String result = checkAndSkipNextToken(Type.IDENTIFIER).getString();

		while (isNextTokenSkipIt(Type.SYMBOL, ".")) {
			result += "." + checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		return result;
	}

	/**
	 * Checks next token and skips it.
	 * @throws IllegalArgumentException if next token is not the one expected
	 */
	private void checkAndSkipNextToken(Type type, String str)
	{
		Token token = tokenizer.skipNextToken();

		if (token.getType() != type || !token.getString().equals(str)) {
			throw new IllegalArgumentException("Expected " + type.name().toLowerCase() + " `" + str + "` but found " + token);
		}
	}

	/**
	 * Returns and skips next token if it's the expected type.
	 * @throws IllegalArgumentException if next token is not of the type expected
	 */
	private Token checkAndSkipNextToken(Type type)
	{
		Token token = tokenizer.skipNextToken();

		if (token.getType() == type) {
			return token;
		} else {
			throw new IllegalArgumentException("Expected a " + type.name().toLowerCase() + " but found " + token);
		}
	}

	/** Skips next token and returns true if matches; just returns false otherwise. */
	private boolean isNextTokenSkipIt(Type type, String str) {
		final boolean result = isNextToken(type, str);
		if (result) tokenizer.skipNextToken();
		return result;
	}

	private boolean isNextToken(Type type, String str) {
		Token token = tokenizer.nextToken();
		return token.getType() == type && token.getString().equals(str);
	}


	// Auxiliary classes

	private static class Condition {

		public String path;
		public Operator operator;
		public Object value;

		public Object getMongoValue() {

			if (operator == Operator.EQ) {
				return value; // EQ has no mongo operator, just use value directly
			} else {
				return MongoUtil.obj(operator.mongoOperator, value);
			}
		}
	}

	enum Operator {

		EQ("=", null),
		NE("!=", QueryOperators.NE),
		LT("<", QueryOperators.LT),
		LTE("<=", QueryOperators.LTE),
		GT(">", QueryOperators.GT),
		GTE(">=", QueryOperators.GTE);

		public final String sqlOperator;
		public final String mongoOperator;

		Operator(String sqlOperator, String mongoOperator) {
			this.sqlOperator = sqlOperator;
			this.mongoOperator = mongoOperator;
		}

		public static Operator fromSqlOperator(String sqlOp) {
			for (Operator op : values()) {
				if (op.sqlOperator.equals(sqlOp)) return op;
			}
			throw new IllegalArgumentException("Operator not supported: " + sqlOp);
		}
	}

	public static class ParseResult
	{
		/** Fields selected (keys are aliases) */
		public Map<String, String> fields = new LinkedHashMap<>(); // To preserve insertion order
		/** Tables used (keys are aliases) */
		public Map<String, String> tables = new HashMap<>();
		/** Cursor obtained after executing collection.find(query, fields) */
		public DBCursor cursor;
	}
}
