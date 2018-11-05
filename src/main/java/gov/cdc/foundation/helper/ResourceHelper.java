package gov.cdc.foundation.helper;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;

@Component
public class ResourceHelper {

	private static final Logger logger = Logger.getLogger(ResourceHelper.class);

	private static String host;
	private static int port;
	private static String database;
	private static String username;
	private static String password;

	private static MongoClient mongo = null;

	private ResourceHelper(@Value("${mongo.host}") String host, @Value("${mongo.port}") int port, @Value("${mongo.user_database}") String database, @Value("${mongo.username}") String username, @Value("${mongo.password}") String password) {
		logger.debug("Creating resource helper...");
		ResourceHelper.host = host;
		ResourceHelper.port = port;
		ResourceHelper.database = database;
		ResourceHelper.username = username;
		ResourceHelper.password = password;
	}

	public static MongoDatabase getDB(String dbName) {
		if (mongo == null) {
			if (StringUtils.isNotEmpty(database) && StringUtils.isNotEmpty(username)) {
				MongoCredential credential = MongoCredential.createCredential(username, database, password.toCharArray());
				
				MongoClientOptions options = MongoClientOptions
					     .builder()
					     .writeConcern(WriteConcern.JOURNALED)
					     .build();
				
				mongo = new MongoClient(new ServerAddress(host, port), credential, options);
				
			} else {
				mongo = new MongoClient(host, port);
			}
		}
		return mongo.getDatabase(dbName);
	}

	public static void closeClient() {
		if (mongo != null)
			mongo.close();
		mongo = null;
	}

}
