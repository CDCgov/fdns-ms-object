package gov.cdc.foundation;

import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;

@SpringBootApplication
@EnableResourceServer
public class ObjectApplication {

	private static final Logger logger = Logger.getLogger(ObjectApplication.class);

	public ObjectApplication() {
		logger.debug("Start application...");
	}

	public static void main(String[] args) {
		SpringApplication.run(ObjectApplication.class, args);
	}
}
