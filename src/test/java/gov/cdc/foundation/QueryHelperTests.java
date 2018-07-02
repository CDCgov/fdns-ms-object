package gov.cdc.foundation;

import static org.assertj.core.api.Assertions.assertThat;

import org.bson.Document;
import org.json.JSONObject;
import org.json.JSONException;
import org.junit.Test;

import gov.cdc.foundation.helper.QueryHelper;

public class QueryHelperTests {

	@Test
	public void testGt() throws JSONException {
		Document query = QueryHelper.buildQuery("foo>5");
		Document test = Document.parse("{ \"foo\": { \"$gt\": 5 } }");
		
		assertThat(query).isEqualTo(test);
	}
	
	@Test
	public void testGte() throws JSONException {
		Document query = QueryHelper.buildQuery("foo>=5");
		Document test = Document.parse("{ \"foo\": { \"$gte\": 5 } }");
		
		assertThat(query).isEqualTo(test);
	}
	
	@Test
	public void testLt() throws JSONException {
		Document query = QueryHelper.buildQuery("foo<5");
		Document test = Document.parse("{ \"foo\": { \"$lt\": 5 } }");
		
		assertThat(query).isEqualTo(test);
	}
	
	@Test
	public void testLte() throws JSONException {
		Document query = QueryHelper.buildQuery("foo<=5");
		Document test = Document.parse("{ \"foo\": { \"$lte\": 5 } }");
		
		assertThat(query).isEqualTo(test);
	}
	
	@Test
	public void testNe() throws JSONException {
		Document query = QueryHelper.buildQuery("foo!:5");
		Document test = Document.parse("{ \"foo\": { \"$ne\": 5 } }");
		
		assertThat(query).isEqualTo(test);
	}
	
	@Test
	public void testNeString() throws JSONException {
		Document query = QueryHelper.buildQuery("foo!:bar");
		Document test = Document.parse("{ \"foo\": { \"$ne\": \"bar\" } }");
		
		assertThat(query).isEqualTo(test);
	}

}
