package gov.cdc.foundation.helper;

import java.util.Iterator;

import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryHelper {
	// check if a number
	private static boolean isNumber(String str) {
		return str.toLowerCase().matches("-?\\d+(\\.\\d+)?");
	}
	
	// check if a boolean
	private static boolean isBoolean(String str) {
		return str.toLowerCase().matches("true|false");
	}
	
	// build a comparison
	private static JSONObject buildComparison(String operand, Object value) throws JSONException {
		JSONObject json = new JSONObject();
		json.put("$" + operand, value);
		return json;
	}
	
	// get the names of a JSONObject
	private static String[] getNames(JSONObject source) {
		int length = source.length();
		if (length == 0) {
			return null;
		}
		Iterator<?> iterator = source.keys();
		String[] names = new String[length];
		int i = 0;
		while (iterator.hasNext()) {
			names[i] = (String) iterator.next();
			i += 1;
		}
		return names;
	}
	
	// merge two JSONObjects
	private static JSONObject mergeObjects(JSONObject source, JSONObject target) throws JSONException {
		for (String key: getNames(source)) {
			Object value = source.get(key);
			if (!target.has(key)) {
				target.put(key, value);
			} else {
				if (value instanceof JSONObject) {
					JSONObject valueJson = (JSONObject)value;
					mergeObjects(valueJson, target.getJSONObject(key));
				} else {
					target.put(key, value);
				}
			}
		}
		return target;
	}
	
	// build and merge a comparison
	private static JSONObject buildAndMergeComparison(String operand, String field, Object raw, JSONObject json) throws JSONException {
		JSONObject comparison = buildComparison(operand, raw);

		// if the field exists then merge it first
		if (json.has(field)) {
			JSONObject merged = mergeObjects(json.getJSONObject(field), comparison);
			return merged;
		} else {
			return comparison;
		}
	}

	// build the query for MongoDB
	public static Document buildQuery(String qs) throws JSONException {
		JSONObject json = new JSONObject();

		// build search terms
		String[] terms = qs.split(" ");
		for (String term : terms) {
			
			if (term.contains(">=")) {
				String[] parts = term.split(">=");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, buildAndMergeComparison("gte", field, Double.parseDouble(raw), json));
			} else if (term.contains("<=")) {
				String[] parts = term.split("<=");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, buildAndMergeComparison("lte", field, Double.parseDouble(raw), json));
			} else if (term.contains(">")) {
				String[] parts = term.split(">");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, buildAndMergeComparison("gt", field, Double.parseDouble(raw), json));
			} else if (term.contains("<")) {
				String[] parts = term.split("<");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, buildAndMergeComparison("lt", field, Double.parseDouble(raw), json));
			} else if (term.contains("!:")) {
				String[] parts = term.split("!:");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, buildAndMergeComparison("ne", field, Double.parseDouble(raw), json));
				else
					json.put(field, buildAndMergeComparison("ne", field, raw, json));
			} else if (term.contains(":")) {
				String[] parts = term.split(":");
				String field = parts[0];
				String raw = parts[1];
				
				// convert types
				if (isNumber(raw))
					json.put(field, Double.parseDouble(raw));
				else if (isBoolean(raw))
					json.put(field, new Boolean(raw));
				else
					json.put(field, raw);
			}
		}

		Document query = Document.parse(json.toString());
		return query;
	}
}
