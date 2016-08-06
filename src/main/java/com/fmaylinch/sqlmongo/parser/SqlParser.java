package com.fmaylinch.sqlmongo.parser;

import com.fmaylinch.sqlmongo.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryOperators;
import org.bson.types.ObjectId;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParser {

	private static final String SELECT = "select ";
	private static final String FROM = " from ";
	private static final String WHERE = " where ";
	private static final String ID = "_id";
	public static final String DATE_PREFIX = "Date('";
	public static final String ID_PREFIX = "Id('";
	public static final String FUN_SUFFIX = "')";

	private static Pattern conditionPattern = Pattern.compile("([a-zA-Z0-9_.]+) *([=!<>]+) *(.+)");

	private static List<SimpleDateFormat> dateFormats =
			Arrays.asList(new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

	private final String querySql;

	public SqlParser(String querySql) {
		this.querySql = querySql.trim();
	}

	public ParseResult parse() {

		ParseResult result = new ParseResult();

		if (!querySql.startsWith(SELECT))
			throw new IllegalArgumentException("Query must start with: select");

		if (!querySql.contains(FROM))
			throw new IllegalArgumentException("Query must contain: from");

		// Get fields ("*" or comma-separated names)

		final String fields = querySql.substring(SELECT.length(), querySql.indexOf(FROM)).trim();

		parseFields(fields, result);

		// Get table and (optional) condition

		final int whereIdx = querySql.indexOf(WHERE, querySql.indexOf(FROM));

		final String table;
		final String conditions;

		if (whereIdx >= 0) {
			table = querySql.substring(querySql.indexOf(FROM) + FROM.length(), whereIdx).trim();
			conditions = querySql.substring(whereIdx + WHERE.length()).trim();
		} else {
			table = querySql.substring(querySql.indexOf(FROM) + FROM.length()).trim();
			conditions = null;
		}

		if (table.contains(" "))
			throw new IllegalArgumentException("Something's wrong with the table name: " + result.table);

		result.table = table;

		parseConditions(conditions, result);

		return result;
	}

	private void parseFields(String fieldsStr, ParseResult result)
	{
		result.fields = MongoUtil.obj();

		if (!fieldsStr.equals("*")) {

			boolean includeId = false;
			result.fieldNames = new ArrayList<>();

			String[] fields = fieldsStr.split(", *");
			for (String field : fields) {
				result.fields.append(field, 1);
				if (field.equals(ID)) includeId = true;
				result.fieldNames.add(field);
			}

			if (!includeId) {
				result.fields.append(ID, 0);
			}
		}
	}

	private void parseConditions(String conditionsStr, ParseResult result)
	{
		result.query = MongoUtil.obj();

		if (conditionsStr == null) return;

		String[] conditions = conditionsStr.split(" +and +");
		for (String conditionStr : conditions) {

			Condition condition = parseCondition(conditionStr);

			result.query.append(condition.field, condition.getMongoValue());
		}
	}

	private Condition parseCondition(String condition)
	{
		Condition result = new Condition();

		Matcher matcher = conditionPattern.matcher(condition);

		if (!matcher.matches())
			throw new IllegalArgumentException("I can't understand this condition: " + condition);

		result.field = matcher.group(1);
		result.operator = Operator.fromSqlOperator(matcher.group(2));
		result.value = parseValue(matcher.group(3).trim());

		return result;
	}

	private Object parseValue(String str) {

		char firstChar = str.charAt(0);
		char lastChar = str.charAt(str.length()-1);

		if (firstChar == '\'' && lastChar == '\'') {
			return str.substring(1, str.length() - 1); // string without quotes

		} else if (str.equals("true") || str.equals("false")) {
			return Boolean.parseBoolean(str);

		} else if ((firstChar >= '0' && firstChar <= '9') || firstChar == '.' || firstChar == '-') {
			return Double.parseDouble(str);

		} else if (str.startsWith(DATE_PREFIX) && str.endsWith(FUN_SUFFIX)) {
			final String dateStr = str.substring(DATE_PREFIX.length(), str.length() - FUN_SUFFIX.length());

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

		} else if (str.startsWith(ID_PREFIX) && str.endsWith(FUN_SUFFIX)) {
			String idStr = str.substring(ID_PREFIX.length(), str.length() - FUN_SUFFIX.length());
			return new ObjectId(idStr);

		} else {
			throw new IllegalArgumentException("Unexpected or not supported value: " + str);
		}
	}

	private static class Condition {

		public String field;
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

	public static class ParseResult {
		public String table;
		/** This is null when retrieving all fields */
		public List<String> fieldNames;
		public BasicDBObject fields;
		public BasicDBObject query;
	}
}
