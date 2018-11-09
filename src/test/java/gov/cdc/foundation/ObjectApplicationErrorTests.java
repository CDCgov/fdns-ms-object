package gov.cdc.foundation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.json.JacksonTester;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

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
					"proxy.hostname=localhost",
					"security.oauth2.resource.user-info-uri=",
					"security.oauth2.protected=",
					"security.oauth2.client.client-id=",
					"security.oauth2.client.client-secret=",
					"ssl.verifying.disable=false"
				})
@AutoConfigureMockMvc
public class ObjectApplicationErrorTests {

	@Autowired
	private TestRestTemplate restTemplate;
	@Autowired
	private MockMvc mvc;
	private String objectCtrlUrl = "/api/1.0/";

	@Before
	public void setup() {
		ObjectMapper objectMapper = new ObjectMapper();
		JacksonTester.initFields(this, objectMapper);
	}

	@Test
	public void createObject1() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>")
				.content("{ 'test' :: '" + (new Date()).getTime() + "' }")
                .contentType("application/json"))
				.andExpect(status().isBadRequest());
	}
	
	@Test
	public void createObject2() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>")
				.contentType("application/json"))
		.andExpect(status().isBadRequest());
	}
	
	@Test
	public void createObject3() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/<na>")
				.content("{ 'test' :: '" + (new Date()).getTime() + "' }")
				.contentType("application/json"))
		.andExpect(status().isBadRequest());
	}
	
	@Test
	public void createObject4() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/<na>")
				.contentType("application/json"))
		.andExpect(status().isBadRequest());
	}
	
	@Test
	public void createObjects() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/multi/<na>/<na>")
				.content("{ 'test' : '" + (new Date()).getTime() + "' }")
                .contentType("application/json"))
				.andExpect(status().isBadRequest());
	}
	
	@Test
	public void getObject() throws Exception {
		ResponseEntity<JsonNode> response = this.restTemplate.getForEntity(objectCtrlUrl + "/{db}/{collection}/{id}", JsonNode.class, "<na>", "<na>", "<na>");
		assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
	}
	
	@Test
	public void countObject1() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/count")
                .content(";;")
                .contentType("application/json"))
				.andExpect(status().isNotFound());
	}
	
	@Test
	public void aggregate1() throws Exception {
		String aggregate = "[ ;; ]";
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/aggregate")
				.content(aggregate)
				.contentType("application/json"))
				.andExpect(status().isNotFound());
	}
	
	@Test
	public void aggregate2() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/aggregate"))
				.andExpect(status().isUnsupportedMediaType());
	}
	
	@Test
	public void queryAllObjects1() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/find")
				.content(";;")
				.contentType("application/json"))
				.andExpect(status().isBadRequest());
	}
	
	@Test
	public void queryAllObjects2() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/find")
				.contentType("application/json"))
		.andExpect(status().isBadRequest());
	}
	
	@Test
	public void distinct1() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/distinct/test")
                .content(";;")
                .contentType("application/json"))
				.andExpect(status().isNotFound());
	}
	
	@Test
	public void distinct2() throws Exception {
		mvc.perform(post(objectCtrlUrl + "/<na>/<na>/distinct/test")
				.contentType("application/json"))
		.andExpect(status().isBadRequest());
	}
	
	
}
