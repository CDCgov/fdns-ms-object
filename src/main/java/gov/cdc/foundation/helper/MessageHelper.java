package gov.cdc.foundation.helper;

import java.util.HashMap;
import java.util.Map;

import gov.cdc.helper.AbstractMessageHelper;

public class MessageHelper extends AbstractMessageHelper {

	public static final String CONST_METHOD = "method";
	public static final String CONST_DATABASE = "db";
	public static final String CONST_COLLECTION = "collectionName";

	public static final String METHOD_INDEX = "index";
	public static final String METHOD_GETOBJECT = "getObject";
	public static final String METHOD_CREATEOBJECT = "createObject";
	public static final String METHOD_CREATEOBJECTS = "createObjects";
	public static final String METHOD_GETCOLLECTION = "getCollection";

	public static final String METHOD_BULKIMPORT = "bulkImport";
	public static final String METHOD_UPDATEOBJECT = "updateObject";
	public static final String METHOD_DELETEOBJECT = "deleteObject";
	public static final String METHOD_DELETECOLLECTION = "deleteCollection";
	public static final String METHOD_QUERY = "query";
	public static final String METHOD_SEARCH = "search";
	public static final String METHOD_AGGREGATE = "aggregate";
	public static final String METHOD_COUNT = "count";
	public static final String METHOD_DISTINCT = "distinct";

	public static final String ERROR_ID_REQUIRED = "The parameter id is required.";
	public static final String ERROR_COLLECTION_REQUIRED = "The parameter collection is required.";
	public static final String ERROR_FIELD_REQUIRED = "The parameter field is required.";
	public static final String ERROR_PAYLOAD_REQUIRED = "The parameter payload is required.";
	public static final String ERROR_QUERYSTRING_REQUIRED = "The parameter qs is required.";

	private MessageHelper() {
		throw new IllegalAccessError("Helper class");
	}

	public static Map<String, Object> initializeLog(String method, String db, String collection) {
		Map<String, Object> log = new HashMap<>();
		log.put(MessageHelper.CONST_METHOD, method);
		log.put(MessageHelper.CONST_DATABASE, db);
		log.put(MessageHelper.CONST_COLLECTION, collection);
		return log;
	}

}
