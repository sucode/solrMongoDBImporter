package org.apache.solr.handler.dataimport;

import java.util.Map;

/**
 * Translate MongoDB's ObjectId to long
 * 
 * @author Eric Yu
 */
public class ObjectIdToLongTransformer extends Transformer {

	private static final String HINT = "hashObjectId";
	@Override
	public Object transformRow(Map<String, Object> row, Context context) {
		for (Map<String, String> fld : context.getAllEntityFields()) {
			String hint = context.replaceTokens(fld.get(HINT));
			if (hint != null && Boolean.parseBoolean(hint)) {
				String column = fld.get(DataImporter.COLUMN);
				Object srcId = row.get(column);
				row.put(column, srcId.hashCode());
			}
		}
		return row;
	}

}
