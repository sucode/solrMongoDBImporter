package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;

import java.util.Map;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataImporter;
import org.apache.solr.handler.dataimport.EntityProcessorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entity Process for MongoDB
 * 
 * @author Eric Yu
 */
public class MongoDBEntityProcessor extends EntityProcessorBase {
	private static final Logger logger = LoggerFactory
			.getLogger(MongoDBEntityProcessor.class);
	private static final String COLLECTION = "collection";
	private static final String COMMAND = "command";
	private static final String DELTA_COMMAND = "deltaCommand";

	protected MongoDBDataSource mongoDBDataSource;
	private String collection;

	@Override
	public void init(Context context) {
		super.init(context);
		collection = context.getEntityAttribute(COLLECTION);
		if (collection == null) {
			throw new DataImportHandlerException(SEVERE, "collection is null");
		}
		mongoDBDataSource = (MongoDBDataSource) context.getDataSource();
	}

	protected void initQuery(String cmd) {
		try {
			DataImporter.QUERY_COUNT.get().incrementAndGet();
			rowIterator = mongoDBDataSource.getData(query, collection);
			this.query = cmd;
		} catch (DataImportHandlerException e) {
			throw e;
		} catch (Exception e) {
			logger.error("query failed: [" + query + "]", e);
			throw new DataImportHandlerException(SEVERE, e);
		}
	}

	@Override
	public Map<String, Object> nextRow() {
		if (rowIterator == null) {
			String cmd = getCommand();
			initQuery(cmd);
		}
		Map<String, Object> data = getNext();
		logger.debug("process: " + data);
		return data;
	}

	public String getCommand() {
		String command = context.getEntityAttribute(COMMAND);
		if (Context.DELTA_DUMP.equals(context.currentProcess())) {
			String deltaCmd = context.getEntityAttribute(DELTA_COMMAND);
			if (deltaCmd == null) {
				logger.warn("in delta mode, but delta command not find, use full command instead");
			} else {
				command = deltaCmd;
			}
		}
		return command;
	}
}
