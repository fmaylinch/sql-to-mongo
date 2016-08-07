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

	/** Query used to find documents, obtained from the WHERE part */
	private BasicDBObject query;
	/** Field selection, obtained from the SELECT part */
	private BasicDBObject select;
	/** Fields selected (keys are aliases) */
	private Map<String, String> fields = new LinkedHashMap<>(); // To preserve insertion order
	/** Tables used (keys are aliases) */
	private Map<String, String> tables = new HashMap<>();
	/** Collection where to find documents, obtained from the FROM part */
	private DBCollection collection;
	/** Cursor obtained after executing collection.find(query, fields) */
	private DBCursor cursor;
	/** Tokenizer used to parse the SQL query */
	private Tokenizer tokenizer;

	public SqlParser(String querySql, DB db) {
		this.querySql = querySql.trim();
		this.db = db;
	}

	public DBCursor getCursor() {
		return cursor;
	}

	public Map<String, String> getFields() {
		return fields;
	}

	/**
	 * After parsing you can get {@link #getCursor()} and {@link #getFields()}
	 */
	public void parse() {

		tokenizer = new Tokenizer(querySql);
		tokenizer.setKeywords(new HashSet<>(Arrays.asList("select", "from", "where", "as", "and", "limit")));

		parseSelect();
		parseFrom();
		if (isNextTokenSkipIt(Type.KEYWORD, "where")) {
			parseWhereConditions();
		}

		cursor = collection.find(query, select);
	}


	// Main parse groups

	private void parseSelect() {

		checkAndSkipNextToken(Type.KEYWORD, "select");

		select = MongoUtil.obj();

		if (isNextToken(Type.SYMBOL, "*")) return; // select all fields, so nothing else to do

		do {
			parseSelectField();
		} while (isNextTokenSkipIt(Type.SYMBOL, ","));

		// Exclude ID if not selected (mongo includes ID by default)
		boolean excludeId = fields.keySet().stream().noneMatch(f -> f.equals(ID));
		if (excludeId) {
			select.append(ID, 0);
		}
	}

	private void parseFrom() {

		checkAndSkipNextToken(Type.KEYWORD, "from");

		String table = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		String alias = table;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		collection = db.getCollection(table);
		tables.put(alias, table);
	}

	private void parseWhereConditions()
	{
		query = MongoUtil.obj();

		do {
			Condition condition = parseCondition();
			query.append(condition.path, condition.getMongoValue());

		} while (isNextTokenSkipIt(Type.KEYWORD, "and"));
	}


	// Piece parsing

	private void parseSelectField() {

		String path = consumeNextPath();
		String alias = path;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		select.append(path, 1);
		fields.put(alias, path);
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
					default: throw new IllegalStateException("Unexpected value: " + token.getString());
				}

			default:
				throw new IllegalStateException("Unexpected token in value: " + token.getType() + " " + token.getString());
		}
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

		throw new IllegalArgumentException("Unsupported date format: " + dateStr
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
	 * @throws IllegalStateException if next token is not the one expected
	 */
	private void checkAndSkipNextToken(Type type, String str)
	{
		Token token = tokenizer.skipNextToken();

		if (token.getType() != type || !token.getString().equals(str)) {
			throw new IllegalStateException("Expected " + type + " " + str + " but found " + token.getType() + " " + token.getString());
		}
	}

	/**
	 * Returns and skips next token if it's the expected type.
	 * @throws IllegalStateException if next token is not of the type expected
	 */
	private Token checkAndSkipNextToken(Type type)
	{
		Token token = tokenizer.skipNextToken();

		if (token.getType() == type) {
			return token;
		} else {
			throw new IllegalStateException("Expected a " + type + " but found " + token.getType() + " " + token.getString());
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
		GTE(">", QueryOperators.GTE);

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
}
