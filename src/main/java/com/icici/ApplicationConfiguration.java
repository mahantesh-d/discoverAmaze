package com.icici;

import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.ReadPreference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.mongodb.MongoClient;
@Configuration
@PropertySource("classpath:config.properties")
@EnableWebMvc
@ComponentScan(basePackages = "com.icici")
@EnableMongoRepositories(basePackages = "com.icici")
public class ApplicationConfiguration {
	
	@Autowired
	private Environment env;

	@Bean
	public MongoDbFactory mongoDbFactory() throws Exception  {

		
//	ServerAddress svaddr = new ServerAddress(env.getProperty("host"), Integer.parseInt(env.getProperty("port")));
		List<ServerAddress> servers = new ArrayList<ServerAddress>();

		try {
			File file = new File("/tmp/mongos.txt");// for AMAZE
//			File file = new File("/tmp/mongosuat.txt"); // for IVR 
//			File file = new File("/app/IBM/websphere/Appserver/profiles/mongos.txt");
			
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				String serverIp = line.split(":")[0];
				int serverPort = Integer.parseInt(line.split(":")[1]); 
				servers.add(new ServerAddress(serverIp,serverPort));
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}


//		servers.add(new ServerAddress("10.158.2.81",27026));
//		servers.add(new ServerAddress("10.158.2.82",27026));
//		servers.add(new ServerAddress("10.158.2.84",27026));
//		servers.add(new ServerAddress("10.158.2.86",27026));

		MongoCredential mongocred = MongoCredential.createCredential(env.getProperty("uname"),
				env.getProperty("authDB"), env.getProperty("password").toCharArray());
		List<MongoCredential> credList = new ArrayList<MongoCredential>();
		credList.add(mongocred);
		

		MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder();

		optionsBuilder.connectTimeout(15000);
		optionsBuilder.socketTimeout(15000);
		optionsBuilder.readPreference(ReadPreference.secondaryPreferred());
		
		MongoClientOptions options = optionsBuilder.build();

		MongoClient mongoClient = new MongoClient(servers, credList, options);

		return new SimpleMongoDbFactory(mongoClient, env.getProperty("database"));
	
	} 

	@Bean
	public MongoTemplate mongoTemplate() throws Exception {
		MongoTemplate mongoTemplate = new MongoTemplate(mongoDbFactory());
		return mongoTemplate;
	}
    
    
    
    
 
}
