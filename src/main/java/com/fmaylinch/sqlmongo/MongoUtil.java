package com.fmaylinch.sqlmongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

import java.net.UnknownHostException;
import java.util.function.Consumer;

public class MongoUtil {

	public static DB connectToDb(String databaseUri)
	{
		try {
			MongoClientURI mongoURI = new MongoClientURI(databaseUri);
			MongoClient client = new MongoClient(mongoURI);
			DB db = client.getDB(mongoURI.getDatabase());

			if (mongoURI.getUsername() != null)
			{
				db.authenticate(mongoURI.getUsername(), mongoURI.getPassword());
			}

			return db;

		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	public static void process(DBCursor cursor, Consumer<DBObject> consumer)
	{
		try {
			while (cursor.hasNext()) {
				consumer.accept(cursor.next());
			}
		}
		finally {
			cursor.close();
		}
	}

	public static BasicDBObject obj(String key, Object value) {
		return new BasicDBObject(key, value);
	}

	public static BasicDBObject obj() {
		return new BasicDBObject();
	}
}
