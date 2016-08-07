package com.fmaylinch.sqlmongo;

import com.codepoetics.protonpack.StreamUtils;
import com.fmaylinch.sqlmongo.parser.SqlParser;
import com.fmaylinch.sqlmongo.util.Fun;
import com.fmaylinch.sqlmongo.util.MongoUtil;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.opencsv.CSVWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SqlMongo {

	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Pattern optionPattern = Pattern.compile("([a-zA-Z0-9]+)=(.+)");
	private static int horizontalPadding;
	private static char csvSeparator;

	public static void main(String[] args) throws IOException {

		// Configure defaults
		Properties config = new Properties();
		config.setProperty("dateFormat", "yyyy-MM-dd HH:mm:ss");
		config.setProperty("output", "horizontal"); // horizontal, vertical or directly a csv file name
		config.setProperty("horizontalPadding", "40"); // only used for horizontal output
		config.setProperty("csvSeparator", ","); // only used for csv output

		try {
			config.load(new FileReader("config.properties"));
		} catch (IOException e) {
			// Ignore
		}

		overrideConfigFromArgs(args, config);

		dateFormat = new SimpleDateFormat(config.getProperty("dateFormat"));
		String output = config.getProperty("output");
		horizontalPadding = Integer.parseInt(config.getProperty("horizontalPadding"));
		csvSeparator = config.getProperty("csvSeparator").charAt(0);

		String uri = getRequiredPropertyWithExample(config, "uri",
				"mongodb://localhost:27017/mydb");
		String querySql = getRequiredPropertyWithExample(config, "query",
				"select userEmail from coupons where couponState = 4");

		DB db = MongoUtil.connectToDb(uri);

		SqlParser parser = new SqlParser(querySql, db);
		parser.parse();

		if (parser.getFields().isEmpty() && !output.equals("vertical")) {
			System.err.println("If you retrieve all fields you must use vertical output. Forcing vertical output.");
			output = "vertical";
		}

		DBCursor cursor = parser.getCursor();

		switch (output) {
			case "horizontal":
				printCursorHorizontal(cursor, parser.getFields());
				break;
			case "vertical":
				printCursorVertical(cursor, parser.getFields());
				break;
			default:
				printCursorToCsv(cursor, output, parser.getFields());
				break;
		}
	}

	private static void printCursorHorizontal(DBCursor cursor, Map<String, String> fields) {

		System.out.println(StringUtils.join(Fun.map(fields.keySet(), f -> StringUtils.rightPad(f, horizontalPadding)), ""));

		MongoUtil.process(cursor, object -> {

			List<String> values = extractValues(object, fields.values());
			System.out.println(StringUtils.join(Fun.map(values, f -> StringUtils.rightPad(f, horizontalPadding)), ""));
		});
	}

	private static void printCursorVertical(DBCursor cursor, Map<String, String> fields) {

		MongoUtil.process(cursor, object -> {

			Collection<String> fieldNames = !fields.isEmpty() ? fields.keySet() : object.keySet();

			List<String> values = extractValues(object, fieldNames);

			List<String> fieldsAndValues = StreamUtils
					.zip(fieldNames.stream(), values.stream(), (f, v) -> f + ": " + v)
					.collect(Collectors.toList());

			System.out.println(StringUtils.join(fieldsAndValues, "\n"));
			System.out.println();
		});
	}

	private static void printCursorToCsv(DBCursor cursor, String csvFile, Map<String, String> fields) throws IOException
	{
		System.out.println("Writing output to CSV file: " + csvFile + " ...");
		CSVWriter writer = new CSVWriter(new FileWriter(csvFile), csvSeparator);

		writer.writeNext(toStringArray(fields.keySet())); // header

		MongoUtil.process(cursor, object -> {

			List<String> values = extractValues(object, fields.values());
			writer.writeNext(toStringArray(values));
		});

		writer.close();
		System.out.println("Done");

	}

	private static List<String> extractValues(DBObject object, Collection<String> fieldNames) {

		return fieldNames.stream().map(f -> extractValue(object, f)).collect(Collectors.toList());
	}

	private static String extractValue(DBObject object, String fieldName)
	{
		if (fieldName.contains(".")) {
			String[] parts = fieldName.split("\\.");
			for (int i = 0; i < parts.length - 1; i++) {
				Object obj = object.get(parts[i]);
				if (obj == null) return "";
				if (!(obj instanceof DBObject)) throw new IllegalArgumentException("Field path is not right: " + fieldName);
				object = (DBObject) obj;
			}
			fieldName = parts[parts.length-1];
		}

		return valueToString(object.get(fieldName));
	}

	private static String valueToString(Object value) {
		if (value == null) return "--";
		if (value instanceof Date) return dateFormat.format(value);
		return value.toString();
	}

	private static String[] toStringArray(Collection<String> list) {
		return list.toArray(new String[list.size()]);
	}

	private static String getRequiredPropertyWithExample(Properties config, String property, String example)
	{
		String value = config.getProperty(property);
		if (StringUtils.isEmpty(value)) {
			System.err.println("Please provide " + property + " through `config.properties` file or command line option e.g. \"" + property + "=" + example + "\"");
			System.exit(0);
		}
		return value;
	}

	private static void overrideConfigFromArgs(String[] args, Properties config)
	{
		for (String arg : args)
		{
			Matcher matcher = optionPattern.matcher(arg);
			if (!matcher.matches())
				throw new IllegalArgumentException("Unexpected option: " + arg + " (format: key=value)");

			config.setProperty(matcher.group(1), matcher.group(2));
		}
	}
}
