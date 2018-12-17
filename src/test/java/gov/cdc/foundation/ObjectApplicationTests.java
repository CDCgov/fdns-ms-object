package gov.cdc.foundation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.hamcrest.CoreMatchers;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.json.JsonContent;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.http.HttpServletResponse;

@RunWith(SpringRunner.class)
@SpringBootTest(
		webEnvironment = WebEnvironment.RANDOM_PORT, 
		properties = { 
					"logging.fluentd.host=fluentd",
					"logging.fluentd.port=24224",
					"mongo.host=mongo",
					"mongo.port=27017",
					"mongo.user_database=",
					"mongo.username=",
					"mongo.password=",
					"immutable=",
					"proxy.hostname=",
					"security.oauth2.resource.user-info-uri=",
					"security.oauth2.protected=",
					"security.oauth2.client.client-id=",
					"security.oauth2.client.client-secret=",
					"ssl.verifying.disable=false"
				})
@AutoConfigureMockMvc
public class ObjectApplicationTests {

	@Autowired
	private TestRestTemplate restTemplate;
	@Autowired
	private MockMvc mvc;
	private JacksonTester<JsonNode> json;
	private String baseUrlPath = "/api/1.0/";
	private String DB_NAME = "test";

	@Before
	public void setup() {
		ObjectMapper objectMapper = new ObjectMapper();
		JacksonTester.initFields(this, objectMapper);
	}

	@Test
	public void indexPage() {
		ResponseEntity<String> response = this.restTemplate.getForEntity("/", String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody(), CoreMatchers.containsString("FDNS Object Microservice"));
	}

	@Test
	public void indexAPI() {
		ResponseEntity<String> response = this.restTemplate.getForEntity(baseUrlPath, String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
		assertThat(response.getBody(), CoreMatchers.containsString("version"));
	}
	
	@Test
	public void testAll() throws Exception {
		String collectionName = Long.toString(Calendar.getInstance().getTime().getTime());
		String id_1 = Long.toString(Calendar.getInstance().getTime().getTime());
		createObject(collectionName, id_1);
		updateObject(collectionName, id_1);
		String id_2 = Long.toString(Calendar.getInstance().getTime().getTime());
		createObject(collectionName, id_2);
		assertThat(countObject(collectionName)).isEqualTo(2);
		assertThat(distinct(collectionName)).isEqualTo(2);
		assertThat(queryAllObjects(collectionName)).isEqualTo(2);
		assertThat(aggregate(collectionName)).isEqualTo(2);
		getObject(collectionName, id_1);
		getObject(collectionName, id_2);
		deleteObject(collectionName, id_2);
		assertThat(countObject(collectionName)).isEqualTo(1);
		assertThat(distinct(collectionName)).isEqualTo(1);
		assertThat(queryAllObjects(collectionName)).isEqualTo(1);
		assertThat(aggregate(collectionName)).isEqualTo(1);
		deleteObject(collectionName, id_1);
		assertThat(countObject(collectionName)).isEqualTo(0);
		assertThat(distinct(collectionName)).isEqualTo(0);
		assertThat(queryAllObjects(collectionName)).isEqualTo(0);
		assertThat(aggregate(collectionName)).isEqualTo(0);
		String id_3 = createObject(collectionName);
		assertThat(countObject(collectionName)).isEqualTo(1);
		assertThat(distinct(collectionName)).isEqualTo(1);
		assertThat(queryAllObjects(collectionName)).isEqualTo(1);
		assertThat(aggregate(collectionName)).isEqualTo(1);
		deleteObject(collectionName, id_3);
		assertThat(countObject(collectionName)).isEqualTo(0);
		assertThat(distinct(collectionName)).isEqualTo(0);
		assertThat(queryAllObjects(collectionName)).isEqualTo(0);
		assertThat(aggregate(collectionName)).isEqualTo(0);
		deleteCollection(collectionName);
	}

	public void createObject(String collection, String id) throws Exception {
		mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/" + id)
				.content("{ 'test' : '" + (new Date()).getTime() + "' }")
                .contentType("application/json"))
				.andExpect(status().isCreated());
	}
	
	public void updateObject(String collection, String id) throws Exception {
		mvc.perform(put(baseUrlPath + "/" + DB_NAME + "/" + collection + "/" + id)
				.content("{ 'test' : '" + (new Date()).getTime() + "' }")
				.contentType("application/json"))
		.andExpect(status().isOk());
	}
	
	public String createObject(String collection) throws Exception {
		MvcResult result = mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection)
				.content("{ 'test' : '" + (new Date()).getTime() + "' }")
				.contentType("application/json"))
				.andExpect(status().isCreated())
				.andReturn();
		return (new JSONObject(result.getResponse().getContentAsString())).getJSONObject("_id").getString("$oid");
	}
	
	public void createObjects(String collection) throws Exception {
		MvcResult result = mvc.perform(post(baseUrlPath + "/multi/" + DB_NAME + "/" + collection)
				.content("[{ 'test' : '" + (new Date()).getTime() + "' },{ 'test' : '" + (new Date()).getTime() + "' }{ 'test' : '" + (new Date()).getTime() + "' }]")
				.contentType("application/json"))
				.andExpect(status().isCreated())
				.andReturn();
		JSONObject body = new JSONObject(result.getResponse().getContentAsString());
		assertThat(body.getInt("inserted") == 3);
		assertThat(body.getJSONArray("ids").length() == 3);
	}
	
	public void getObject(String collection, String id) throws Exception {
		ResponseEntity<JsonNode> response = this.restTemplate.getForEntity(baseUrlPath + "/{db}/{collection}/{id}", JsonNode.class, DB_NAME, collection, id);
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathStringValue("@._id");
		assertThat(body).hasJsonPathStringValue("@.test");
		assertThat(body).extractingJsonPathStringValue("@._id").isEqualTo(id);
	}
	
	public int countObject(String collection) throws Exception {
		Boolean isEmpty = aggregate(collection) == -1 ? true:false;
		MvcResult result;
		if(isEmpty) {
			mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/count")
					.content("{}")
					.contentType("application/json"))
					.andExpect(status().isNotFound())
					.andReturn();
			return -1;
		}else {
			result = mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/count")
					.content("{}")
					.contentType("application/json"))
					.andExpect(status().isOk())
					.andReturn();
			return (new JSONObject(result.getResponse().getContentAsString())).getInt("count");
		}
	}
	
	public int aggregate(String collection) throws Exception {
		String aggregate = "[ { $group: { _id: '$test', count: { $sum: 1 } } }, { $sort: { count: -1  } } ]";
		MvcResult result = mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/aggregate")
				.content(aggregate)
				.contentType("application/json"))
				.andReturn();
		if(result.getResponse().getStatus() == HttpStatus.OK.value()){
			assertTrue(result.getResponse().getStatus() == HttpStatus.OK.value());
			return (new JSONObject(result.getResponse().getContentAsString())).getJSONArray("items").length();
		}else{
			assertTrue(result.getResponse().getStatus() == HttpStatus.NOT_FOUND.value());
			return -1;
		}
	}
	
	public int queryAllObjects(String collection) throws Exception {
		MvcResult result = mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/find")
				.content("{}")
				.contentType("application/json"))
				.andExpect(status().isOk())
				.andReturn();
		return (new JSONObject(result.getResponse().getContentAsString())).getInt("total");
	}
	
	public int distinct(String collection) throws Exception {
		MvcResult result = mvc.perform(post(baseUrlPath + "/" + DB_NAME + "/" + collection + "/distinct/test")
                .content("{}")
                .contentType("application/json"))
				.andExpect(status().isOk())
				.andReturn();
		return (new JSONArray(result.getResponse().getContentAsString())).length();
	}
	
	public void deleteObject(String collection, String id) throws IOException {
		ResponseEntity<JsonNode> response = this.restTemplate.exchange(baseUrlPath + "/{db}/{collection}/{id}", HttpMethod.DELETE, null, JsonNode.class, DB_NAME, collection, id);
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
		assertThat(body).hasJsonPathBooleanValue("@.success");
		assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(true);
	}
	
	public void deleteCollection(String collection) throws Exception {
		Boolean isEmpty = aggregate(collection) == -1 ? true:false;
		ResponseEntity<JsonNode> response = this.restTemplate.exchange(baseUrlPath + "/{db}/{collection}", HttpMethod.DELETE, null, JsonNode.class, DB_NAME, collection);
		JsonContent<JsonNode> body = this.json.write(response.getBody());
		if(isEmpty){
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
			assertThat(body).hasJsonPathBooleanValue("@.success");
			assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(false);
		}else {
			assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
			assertThat(body).hasJsonPathBooleanValue("@.success");
			assertThat(body).extractingJsonPathBooleanValue("@.success").isEqualTo(true);
		}
	}
	
}
