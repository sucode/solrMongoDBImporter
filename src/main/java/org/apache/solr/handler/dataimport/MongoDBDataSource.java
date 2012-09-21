package org.apache.solr.handler.dataimport;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * MongoDB DataSource
 * 
 * @author Eric Yu
 */
public class MongoDBDataSource extends
		DataSource<Iterator<Map<String, Object>>> {
	private static final Logger logger = LoggerFactory
			.getLogger(MongoDBDataSource.class);

	private static final String F_MONGO_HOST = "host";
	private static final String F_MONGO_PORT = "port";
	private static final String F_MONGO_USERNAME = "username";
	private static final String F_MONGO_PASSWORD = "password";
	private static final String F_DATABASE = "database";

	private static final String DEFAULT_HOST = "localhost";
	private static final String DEFAULT_PORT = "27017";

	protected DB mongo;
	private DBCollection collection;
	private DBCursor cursor;

	@Override
	public void init(Context context, Properties initProps) {
		String host = initProps.getProperty(F_MONGO_HOST, DEFAULT_HOST);
		int port = Integer.parseInt(initProps.getProperty(F_MONGO_PORT,
				DEFAULT_PORT));
		String username = initProps.getProperty(F_MONGO_USERNAME);
		String password = initProps.getProperty(F_MONGO_PASSWORD);
		String dbName = initProps.getProperty(F_DATABASE);

		if (dbName == null) {
			throw new DataImportHandlerException(SEVERE,
					"database can not be null");
		}

		try {
			Mongo mongo = new Mongo(host, port);
			this.mongo = mongo.getDB(dbName);

			if (username != null) {
				boolean auth = this.mongo.authenticate(username,
						password.toCharArray());
				if (!auth) {
					throw new DataImportHandlerException(SEVERE,
							"auth failed with username: " + username
									+ ", password: " + password);
				}
			}
			logger.info(String.format("mongodb [%s:%d]@%s inited", host, port, dbName));
		} catch (Exception e) {
			throw new DataImportHandlerException(SEVERE, "init mongodb failed");
		}
	}

	@Override
	public Iterator<Map<String, Object>> getData(String query) {
		DBObject queryObject = (DBObject) JSON.parse(query);
		
		cursor = collection.find(queryObject);
		ResultSetIterator resultSet = new ResultSetIterator(cursor);
		return resultSet.getIterator();
	}

	public Iterator<Map<String, Object>> getData(String query,
			String collectionName) {
		logger.info(String.format("query mongodb with cmd: %s at collection: %s", query, collectionName));
		this.collection = mongo.getCollection(collectionName);
		return getData(query);
	}

	private class ResultSetIterator {
		DBCursor mCursor;

		Iterator<Map<String, Object>> resultSet;

		public ResultSetIterator(DBCursor MongoCursor) {
			this.mCursor = MongoCursor;

			resultSet = new Iterator<Map<String, Object>>() {
				public boolean hasNext() {
					return hasnext();
				}

				public Map<String, Object> next() {
					return getNext();
				}

				public void remove() {
				}
			};

		}

		public Iterator<Map<String, Object>> getIterator() {
			return resultSet;
		}

		private Map<String, Object> getNext() {
			DBObject mongoObject = getMongoCursor().next();
			
			Set<String> keys = mongoObject.keySet();
			Map<String, Object> result = new HashMap<String, Object>(keys.size());
			
			for (String key : keys) {
				Object value = mongoObject.get(key);
				result.put(key, value);
			}

			return result;
		}

		private boolean hasnext() {
			if (mCursor == null)
				return false;
			try {
				if (mCursor.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			} catch (MongoException e) {
				close();
				wrapAndThrow(SEVERE, e);
				return false;
			}
		}

		private void close() {
			try {
				if (mCursor != null)
					mCursor.close();
			} catch (Exception e) {
				logger.warn("Exception while closing result set", e);
			} finally {
				mCursor = null;
			}
		}
	}

	private DBCursor getMongoCursor() {
		return this.cursor;
	}

	@Override
	public void close() {
		if (this.cursor != null) {
			this.cursor.close();
		}

	}

}
