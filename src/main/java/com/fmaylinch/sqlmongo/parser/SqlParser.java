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
import com.mongodb.DBObject;
import com.mongodb.QueryOperators;
import org.bson.types.ObjectId;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SqlParser {

	private static List<SimpleDateFormat> dateFormats =
			Arrays.asList(new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

	private final String querySql;
	private final DB db;

	/** Tokenizer used to parse the SQL query */
	private Tokenizer tokenizer;

	private Result result;


	public SqlParser(String querySql, DB db) {
		this.querySql = querySql.trim();
		this.db = db;
	}

	/**
	 * Parses the SQL query
	 */
	public Result parse() {

		result = new Result();

		tokenizer = new Tokenizer(querySql);
		tokenizer.setKeywords(new HashSet<>(Arrays.asList(
				"select", "from", "where", "as", "and", "limit", "order", "by", "asc", "desc", "join", "on")));

		final BasicDBObject select = parseSelect();

		result.main = parseFrom();

		if (isNextToken(Type.KEYWORD, "join")) {
			result.join = parseJoin();
			result.join.fields = filterFields(select, result.join.collectionAlias);
		}

		result.main.query = parseWhere();
		result.main.fields = filterFields(select, result.main.collectionAlias);

		if (result.join != null) {
			// Include necessary fields used in join condition. TODO: clean this (well, and the whole thing!)
			for (Object value : result.join.query.values()) {
				if (value instanceof FieldReference) {
					result.main.fields.append(((FieldReference) value).path, 1);
				}
			}
		}

		result.main.initCursor();

		if (isNextTokenSkipIt(Type.KEYWORD, "order")) {
			checkAndSkipNextToken(Type.KEYWORD, "by");
			parseOrders();
		}

		if (isNextTokenSkipIt(Type.KEYWORD, "limit")) {
			parseLimit();
		}

		return result;
	}


	// Main parse groups

	private BasicDBObject parseSelect() {

		checkAndSkipNextToken(Type.KEYWORD, "select");

		BasicDBObject select = MongoUtil.obj();

		if (isNextTokenSkipIt(Type.SYMBOL, "*")) return select;

		do {
			parseSelectField(select);
		} while (isNextTokenSkipIt(Type.SYMBOL, ","));

		return select;
	}

	private CollectionInfo parseFrom() {

		checkAndSkipNextToken(Type.KEYWORD, "from");

		return parseTableNameAndAlias();
	}

	private CollectionInfo parseJoin() {

		checkAndSkipNextToken(Type.KEYWORD, "join");

		CollectionInfo info = parseTableNameAndAlias();

		checkAndSkipNextToken(Type.KEYWORD, "on");

		info.query = parseConditions(true);

		return info;
	}

	private BasicDBObject parseWhere()
	{
		if (isNextTokenSkipIt(Type.KEYWORD, "where"))
		{
			return parseConditions(false);
		} else {
			return MongoUtil.obj();
		}
	}

	private BasicDBObject parseConditions(boolean allowPathValue)
	{
		BasicDBObject query = MongoUtil.obj();

		do {
			Condition condition = parseCondition(allowPathValue);
			query.append(condition.path, condition.getMongoValue());

		} while (isNextTokenSkipIt(Type.KEYWORD, "and"));

		return query;
	}

	private void parseOrders()
	{
		BasicDBObject orders = MongoUtil.obj();

		do {

			String path = parsePath();
			int direction = 1; // asc by default
			if (isNextTokenSkipIt(Type.KEYWORD, "asc")) {
				direction = 1;
			} else if (isNextTokenSkipIt(Type.KEYWORD, "desc")) {
				direction = -1;
			}

			orders.append(path, direction);

		} while (isNextTokenSkipIt(Type.SYMBOL, ","));

		result.main.cursor.sort(orders);
	}

	private void parseLimit()
	{
		final Token numberToken = checkAndSkipNextToken(Type.NUMBER);
		result.main.cursor.limit(Integer.parseInt(numberToken.getString()));
	}


	// Piece parsing

	private void parseSelectField(BasicDBObject select) {

		String path = parsePath();
		String alias = path;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		select.append(path, 1);
		result.fields.put(alias, path);
	}

	private CollectionInfo parseTableNameAndAlias()
	{
		String table = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		String alias = table;

		if (isNextTokenSkipIt(Type.KEYWORD, "as")) {
			alias = checkAndSkipNextToken(Type.IDENTIFIER).getString();
		}

		final CollectionInfo colInfo = new CollectionInfo();
		colInfo.collectionAlias = alias;
		colInfo.collection = db.getCollection(table);
		return colInfo;
	}

	/**
	 * Returns an object containing only the fields that have the given alias prefix (the alias prefix is removed).
	 */
	private BasicDBObject filterFields(BasicDBObject object, String alias)
	{
		BasicDBObject result = MongoUtil.obj();

		String aliasDot = alias + ".";

		for (String field : object.keySet()) {
			if (field.startsWith(aliasDot)) {
				String fieldWithoutAlias = field.substring(aliasDot.length());
				result.append(fieldWithoutAlias, object.get(field));
			}
		}
		return result;
	}

	private Condition parseCondition(boolean allowPathValue)
	{
		Condition result = new Condition();

		result.path = parsePath();
		result.operator = Operator.fromSqlOperator(checkAndSkipNextToken(Type.SYMBOL).getString());
		result.value = parseValue(allowPathValue);

		return result;
	}

	private Object parseValue(boolean allowPathValue) {

		Token token = tokenizer.nextToken();

		switch (token.getType()) {
			case STRING: return skipNextString();
			case NUMBER: return Double.parseDouble(tokenizer.skipNextToken().getString());
			case BOOLEAN: return Boolean.parseBoolean(tokenizer.skipNextToken().getString());
			case IDENTIFIER:
				switch (token.getString()) {
					case "Date": return parseDateArgument();
					case "Id": return parseIdArgument();
					default: if (allowPathValue) return new FieldReference(parsePath());
				}
		}

		throw new IllegalArgumentException("Unexpected value: " + token);
	}

	private Date parseDateArgument() {

		checkAndSkipNextToken(Type.IDENTIFIER, "Date");
		checkAndSkipNextToken(Type.SYMBOL, "(");
		String dateStr = skipNextString();
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
		checkAndSkipNextToken(Type.IDENTIFIER, "Id");
		checkAndSkipNextToken(Type.SYMBOL, "(");
		String idStr = skipNextString();
		checkAndSkipNextToken(Type.SYMBOL, ")");
		return new ObjectId(idStr);
	}


	// Tokenizer helper methods

	private String skipNextString() {
		String stringWithQuotes = checkAndSkipNextToken(Type.STRING).getString();
		return stringWithQuotes.substring(1, stringWithQuotes.length()-1);
	}

	/** Joins next path made of IDENTIFIERs and dots like house.address.number */
	public String parsePath()
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

	public static class Result implements Iterable<Result.Doc>
	{
		/** Fields selected (keys are aliases) */
		public Map<String, String> fields = new LinkedHashMap<>(); // To preserve insertion order

		/** Main collection */
		private CollectionInfo main;

		/** Joined collection, for queries with join */
		private CollectionInfo join;

		public class Doc {

			/** Objects indexed by collection alias */
			private final Map<String, DBObject> objMap;

			public Doc(Map<String, DBObject> objMap) {
				this.objMap = objMap;
			}

			public Object getValue(String fieldPath)
			{
				String[] parts = fieldPath.split("\\.");

				DBObject object = objMap.get(parts[0]);

				for (int i = 1; i < parts.length - 1; i++) {
					Object obj = object.get(parts[i]);
					if (obj == null) return null;
					if (!(obj instanceof DBObject)) throw new IllegalArgumentException("Field path is not right: " + fieldPath);
					object = (DBObject) obj;
				}

				return object.get(parts[parts.length-1]);
			}

			/** Gets all fields names, prefixed with the corresponding alias so they can be used in {@link #getValue(String)} */
			public List<String> getFieldNames() {
				return objMap.keySet().stream()
						.flatMap(alias ->
								objMap.get(alias).keySet().stream().map(k -> alias + "." + k))
						.collect(Collectors.toList());
			}
		}

		private class ParseResultIterator implements Iterator<Doc>
		{
			private DBObject mainObj;
			private DBObject joinObj;

			@Override
			public boolean hasNext() {
				return main.cursor.hasNext() || hasNextJoinObject();
			}

			private boolean hasNextJoinObject() {
				return join != null && join.cursor != null && join.cursor.hasNext();
			}

			@Override
			public Doc next()
			{
				if (hasNextJoinObject()) {
					joinObj = join.cursor.next();
				} else {
					mainObj = main.cursor.next();
					if (join != null) {
						join.initCursor(mainObj);
						joinObj = join.cursor.hasNext() ? join.cursor.next() : MongoUtil.obj();
					}
				}

				Map<String,DBObject> objMap = new HashMap<>();
				objMap.put(main.collectionAlias, mainObj);
				if (join != null) {
					objMap.put(join.collectionAlias, joinObj);
				}

				return new Doc(objMap);
			}
		}

		@Override
		public Iterator<Doc> iterator() {
			return new ParseResultIterator();
		}
	}

	private static class CollectionInfo
	{
		public DBCursor cursor;
		public DBCollection collection;
		public String collectionAlias;
		public BasicDBObject query;
		public BasicDBObject fields;

		public void initCursor() {
			cursor = collection.find(query, fields);
		}

		public void initCursor(DBObject referred) {
			DBObject populatedQuery = populateReferences(query, referred);
			cursor = collection.find(populatedQuery, fields);
		}

		private DBObject populateReferences(BasicDBObject object, DBObject referred)
		{
			BasicDBObject result = MongoUtil.obj();
			for (String field : object.keySet()) {
				Object value = object.get(field);
				if (value instanceof FieldReference) {
					// TODO: we should do like Doc.getValue (split path)
					result.append(field, referred.get(((FieldReference) value).path)); // Get referred value
				} else {
					result.append(field, object.get(field)); // Just copy value
				}
			}
			return result;
		}
	}

	private static class FieldReference {

		public final String path;

		public FieldReference(String path) {
			this.path = path;
		}
	}
}
