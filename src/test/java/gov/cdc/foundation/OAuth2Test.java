package gov.cdc.foundation;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import gov.cdc.helper.RequestHelper;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.boot.web.server.LocalServerPort;

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
					"security.oauth2.resource.user-info-uri=http://fdns-ms-stubbing:3002/oauth2/token/introspect",
					"security.oauth2.protected=/api/1.0/**",
					"security.oauth2.client.client-id=test",
					"security.oauth2.client.client-secret=testsecret",
					"ssl.verifying.disable=false"
				})
@AutoConfigureMockMvc
public class OAuth2Test {

	@LocalServerPort
	private int port;

	@Autowired
	private TestRestTemplate restTemplate;
	private String baseUrlPath = "/api/1.0/";
	private String localBasePath = "http://localhost:";

	@Test
	public void indexPage() {
		ResponseEntity<String> response = this.restTemplate.getForEntity(baseUrlPath, String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(200);
	}

	@Test
	public void testUnauthenticated() {
		ResponseEntity<String> response = this.restTemplate.getForEntity(baseUrlPath + "mydb/mycollection", String.class);
		assertThat(response.getStatusCodeValue()).isEqualTo(401);
	}

	@Test
	public void testAthenticatedSpecific() {
		setScope("fdns.object fdns.object.mydb.mycollection.create");
		
		ResponseEntity<String> response = RequestHelper.getInstance("Bearer testtoken")
				.executePost(
					localBasePath + port + baseUrlPath + "mydb/mycollection", 
					"{ \"foo\": \"bar\"}",
					MediaType.APPLICATION_JSON
				);

		assertThat(response.getStatusCodeValue()).isEqualTo(201);
	}

	@Test
	public void testAthenticatedWildcard() {
		setScope("fdns.object fdns.object.*.*.*");
		
		ResponseEntity<String> response = RequestHelper.getInstance("Bearer testtoken")
				.executePost(
					localBasePath + port + baseUrlPath + "mydb/mycollection", 
					"{ \"foo\": \"bar\"}",
					MediaType.APPLICATION_JSON
				);

		assertThat(response.getStatusCodeValue()).isEqualTo(201);
	}

	private void setScope(String scope) {
		MultiValueMap<String, String> data = new LinkedMultiValueMap<>();
		data.add("scope", scope);
		RequestHelper.getInstance().executePost("http://fdns-ms-stubbing:3002/oauth2/mock", data);
	}
}
