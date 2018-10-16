package gov.cdc.foundation.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.result.DeleteResult;

import gov.cdc.foundation.helper.LoggerHelper;
import gov.cdc.foundation.helper.MessageHelper;
import gov.cdc.foundation.helper.ResourceHelper;
import gov.cdc.foundation.helper.QueryHelper;
import gov.cdc.helper.ErrorHandler;
import gov.cdc.helper.common.ServiceException;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Controller
@RequestMapping("/api/1.0/")
public class ObjectController {

	private static final Logger logger = Logger.getLogger(ObjectController.class);
	
	@Value("${version}")
	private String version;

	@Value("${immutable}")
	private String immutableCollections;

	public ObjectController() {
	}
	
	@RequestMapping(method = RequestMethod.GET)
	@ResponseBody
	public ResponseEntity<?> index() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		
		Map<String, Object> log = new HashMap<>();
		
		try {
			JSONObject json = new JSONObject();
			json.put("version", version);
			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_INDEX, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}


	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.GET, value = "/{db}/{collection}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Get object", notes = "Get object")
	@ResponseBody
	public ResponseEntity<?> getObject(@ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, @ApiParam(value = "Object Id") @PathVariable(value = "id") String id) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_GETOBJECT, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {

			if (id == null || id.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_ID_REQUIRED);
			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);

			Object objectId = getId(id);
			FindIterable<Document> results = coll.find(new BasicDBObject("_id", objectId));
			if (results.iterator().hasNext())
				return new ResponseEntity<>(mapper.readTree(results.first().toJson()), HttpStatus.OK);
			else
				throw new ServiceException("The object " + id + " doesn't exist in the collection " + collection);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_GETOBJECT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Create object", notes = "Create object")
	@ResponseBody
	public ResponseEntity<?> createObject(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db", required = true) String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection", required = true) String collection) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_CREATEOBJECT, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {
			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document doc = Document.parse(payload);
			coll.insertOne(doc);
			return new ResponseEntity<>(mapper.readTree(doc.toJson()), HttpStatus.CREATED);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_CREATEOBJECT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}
	
	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.POST, value = "/multi/{db}/{collection}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Create a list of objects", notes = "Create a list of objects")
	@ResponseBody
	public ResponseEntity<?> createObjects(@ApiParam(value = "Array of JSON objects") @RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db", required = true) String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection", required = true) String collection) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_CREATEOBJECTS, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {
			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			JSONArray items = new JSONArray(payload);
			List<Document> documents = new ArrayList<>();
			for (int i = 0; i < items.length(); i++) {
				JSONObject item = items.getJSONObject(i);
				documents.add(Document.parse(item.toString()));
			}
			coll.insertMany(documents);
			
			// Build the JSON output
			JSONObject out = new JSONObject();
			out.put("inserted", documents.size());
			JSONArray elementIds = new JSONArray();
			for (Document document : documents) {
				elementIds.put(document.getObjectId("_id"));
			}
			out.put("ids", elementIds);
			
			return new ResponseEntity<>(mapper.readTree(out.toString()), HttpStatus.CREATED);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_CREATEOBJECTS, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}
	
	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/bulk/{db}/{collection}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Bulk import of objects from a CSV file", notes = "Bulk import of objects from a CSV file")
	@ResponseBody
	public ResponseEntity<?> bulkImport(@ApiParam(value = "CSV file") @RequestParam("csv") MultipartFile csvFile, @ApiParam(value = "Database name") @PathVariable(value = "db", required = true) String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection", required = true) String collection, @ApiParam(value = "CSV format", allowableValues="Default,Excel,MySQL,RFC4180,TDF", defaultValue="Default") @RequestParam(value = "csvFormat", required = true) String csvFormat) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_BULKIMPORT, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {
			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			List<Document> documents = new ArrayList<>();
			List<String> headers = new ArrayList<>();
			CSVParser parser = null;
			
			// Resources should be closed
			try {
				parser = CSVParser.parse(IOUtils.toString(csvFile.getInputStream()), CSVFormat.valueOf(csvFormat));
			}  catch (Exception e) {
				logger.error(e);
				LoggerHelper.log(MessageHelper.METHOD_BULKIMPORT, log);
				
				return ErrorHandler.getInstance().handle(e, log);
			} finally {
				if (parser != null)
					parser.close();
			}
			
			for (CSVRecord csvRecord : parser) {
				if (headers.isEmpty()) {
					for (int i = 0; i < csvRecord.size(); i++)
						headers.add(csvRecord.get(i));
				} else {
					Document doc = new Document();
					for (int i = 0; i < csvRecord.size(); i++) {
						if (i < headers.size()) {
							doc.put(headers.get(i), csvRecord.get(i));
						}
					}
					documents.add(doc);
				}
			}
			
			coll.insertMany(documents);
			
			// Build the JSON output
			JSONObject out = new JSONObject();
			out.put("inserted", documents.size());
			JSONArray elementIds = new JSONArray();
			for (Document document : documents) {
				elementIds.put(document.getObjectId("_id"));
			}
			out.put("ids", elementIds);
			
			return new ResponseEntity<>(mapper.readTree(out.toString()), HttpStatus.CREATED);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_BULKIMPORT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/{id}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Create object with id", notes = "Create object with id")
	@ResponseBody
	public ResponseEntity<?> createObjectWithId(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, @ApiParam(value = "Object Id") @PathVariable(value = "id") String id) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_CREATEOBJECT, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (id == null || id.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_ID_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document doc = Document.parse(payload);
			doc.append("_id", id);
			coll.insertOne(doc);
			return new ResponseEntity<>(mapper.readTree(doc.toJson()), HttpStatus.CREATED);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_CREATEOBJECT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.PUT, value = "/{db}/{collection}/{id}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Update object", notes = "Update object")
	@ResponseBody
	public ResponseEntity<?> updateObject(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, @ApiParam(value = "Object Id") @PathVariable(value = "id") String id) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_UPDATEOBJECT, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (id == null || id.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_ID_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			if (isImmutableCollection(collection))
				throw new ServiceException("The following collection is immutable: " + collection);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document doc = Document.parse(payload);

			Object objectId = getId(id);
			coll.replaceOne(new BasicDBObject("_id", objectId), doc);
			return new ResponseEntity<>(mapper.readTree(doc.toJson()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_UPDATEOBJECT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection)) or (#db == 'settings')")
	@RequestMapping(method = RequestMethod.DELETE, value = "/{db}/{collection}/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Delete object", notes = "Delete object")
	@ResponseBody
	public ResponseEntity<?> deleteObject(@ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, @ApiParam(value = "Object Id") @PathVariable(value = "id") String id) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DELETEOBJECT, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (id == null || id.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_ID_REQUIRED);

			if (isImmutableCollection(collection))
				throw new ServiceException("The following collection is immutable: " + collection);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);

			Object objectId = getId(id);
			DeleteResult result = coll.deleteOne(new BasicDBObject("_id", objectId));
			JSONObject json = new JSONObject();
			json.put(MessageHelper.CONST_SUCCESS, true);
			json.put("deleted", result.getDeletedCount());
			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DELETEOBJECT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.DELETE, value = "/{db}/{collection}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Delete collection", notes = "Delete collection")
	@ResponseBody
	public ResponseEntity<?> deleteCollection(@ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DELETECOLLECTION, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);

			if (isImmutableCollection(collection))
				throw new ServiceException("The following collection is immutable: " + collection);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			coll.drop();

			JSONObject json = new JSONObject();
			json.put(MessageHelper.CONST_SUCCESS, true);
			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DELETECOLLECTION, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}
	
	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/search", produces = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Search object(s)", notes = "Search object(s)")
	@ResponseBody
	public ResponseEntity<?> search(
		@ApiParam(value = "Database name") @PathVariable(value = "db") String db,
		@ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection,
		@ApiParam(value = "Search query") @RequestParam(value = "qs", defaultValue = "") String qs,
		@ApiParam(value = "Set the starting point of the result set") @RequestParam(value = "from", defaultValue = "0") int from,
		@ApiParam(value = "Limit the number of objects to return") @RequestParam(value = "size", defaultValue = "-1") int size,
		@ApiParam(value = "Field used to order the result set") @RequestParam(value = "sort", required = false) String sort,
		@ApiParam(value = "Ascending/descending order") @RequestParam(value = "order", defaultValue = "1") int order
	) {
		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_SEARCH, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {
			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document query = QueryHelper.buildQuery(qs);

			Object json = executeQuery(coll, query, from, size, sort, order);

			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_QUERY, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/find", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Find object(s)", notes = "Find object(s)")
	@ResponseBody
	public ResponseEntity<?> query(
		@RequestBody String payload, 
		@ApiParam(value = "Database name") @PathVariable(value = "db") String db, 
		@ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, 
		@ApiParam(value = "Set the starting point of the result set") @RequestParam(value = "from", defaultValue = "0") int from, 
		@ApiParam(value = "Limit the number of objects to return") @RequestParam(value = "size", defaultValue = "-1") int size, 
		@ApiParam(value = "Field used to order the result set") @RequestParam(value = "sort", required = false) String sort, 
		@ApiParam(value = "Ascending/descending order") @RequestParam(value = "order", defaultValue = "1") int order
	) {
		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_QUERY, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {
			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document query = Document.parse(payload);

			Object json = executeQuery(coll, query, from, size, sort, order);

			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_QUERY, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	private JSONObject executeQuery(MongoCollection<Document> coll, Document query, int from, int size, String sort, int order) throws ServiceException {
		JSONArray results = new JSONArray();
		JSONObject json = new JSONObject();

		FindIterable<Document> it = coll.find(query);
		if (from > 0)
			it.skip(from);
		if (size > 0)
			it.limit(size);
		if (sort != null && sort.length() > 0)
			it.sort(new Document(sort, order));
		try {
			MongoCursor<Document> iterator = it.iterator();
			while (iterator.hasNext())
				results.put(new JSONObject(iterator.next().toJson()));

			json.put("items", results);
			json.put("total", coll.count(query));
			if (from > 0)
				json.put("from", from);
			if (size > 0)
				json.put("size", size);
		} catch (Exception e) {
			throw new ServiceException(e);
		}
		return json;
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/aggregate", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Aggregate", notes = "Aggregate")
	@ResponseBody
	public ResponseEntity<?> aggregate(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_AGGREGATE, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);

			JSONArray pipelines = new JSONArray(payload);
			List<Document> pps = new ArrayList<>();
			for (int i = 0; i < pipelines.length(); i++)
				pps.add(Document.parse(pipelines.getJSONObject(i).toString()));

			AggregateIterable<Document> it = coll.aggregate(pps);
			MongoCursor<Document> iterator = it.iterator();

			JSONArray results = new JSONArray();
			JSONObject json = new JSONObject();

			while (iterator.hasNext())
				results.put(new JSONObject(iterator.next().toJson()));

			json.put("items", results);

			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_AGGREGATE, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/count", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Count object(s)", notes = "Count object(s)")
	@ResponseBody
	public ResponseEntity<?> count(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_COUNT, db, collection);

		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document query = Document.parse(payload);

			JSONObject json = new JSONObject();
			json.put(MessageHelper.METHOD_COUNT, coll.count(query));

			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log("count", log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	@PreAuthorize("!@authz.isSecured() or #oauth2.hasScope('object.'.concat(#db).concat('.').concat(#collection))")
	@RequestMapping(method = RequestMethod.POST, value = "/{db}/{collection}/distinct/{field}", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
	@ApiOperation(value = "Get distinct values", notes = "Get distinct values")
	@ResponseBody
	public ResponseEntity<?> distinct(@RequestBody String payload, @ApiParam(value = "Database name") @PathVariable(value = "db") String db, @ApiParam(value = "Collection name") @PathVariable(value = "collection") String collection, @ApiParam(value = "Field name") @PathVariable(value = "field") String field) {

		Map<String, Object> log = MessageHelper.initializeLog(MessageHelper.METHOD_DISTINCT, db, collection);
		ObjectMapper mapper = new ObjectMapper();

		try {

			if (collection == null || collection.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_COLLECTION_REQUIRED);
			if (field == null || field.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_FIELD_REQUIRED);
			if (payload == null || payload.length() == 0)
				throw new ServiceException(MessageHelper.ERROR_PAYLOAD_REQUIRED);

			MongoCollection<Document> coll = ResourceHelper.getDB(db).getCollection(collection);
			Document query = Document.parse(payload);

			DistinctIterable<String> results = coll.distinct(field, query, String.class);
			MongoCursor<String> it = results.iterator();

			JSONArray json = new JSONArray();
			while (it.hasNext())
				json.put(it.next());

			return new ResponseEntity<>(mapper.readTree(json.toString()), HttpStatus.OK);
		} catch (Exception e) {
			logger.error(e);
			LoggerHelper.log(MessageHelper.METHOD_DISTINCT, log);
			
			return ErrorHandler.getInstance().handle(e, log);
		}
	}

	private boolean isImmutableCollection(String collection) {
		return Arrays.asList(immutableCollections.split(";")).contains(collection);
	}

	private Object getId(String id) {
		Object objectId = id;
		try {
			objectId = new ObjectId(id);
		} catch (Exception e) {
			// Do nothing
			logger.debug(e);
		}
		return objectId;
	}
}