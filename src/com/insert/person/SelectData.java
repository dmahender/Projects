package com.insert.person;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;



public class SelectData {
	 private static final Logger LOG = LoggerFactory.getLogger(SelectData.class);

	public static void main(String[] args) throws IOException {
	
		String query = "SELECT * FROM dupperson";
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		Session session = cluster.connect("demo");
		Metadata metadata = cluster.getMetadata();
		   System.out.printf("Connected to cluster: %s\n", 
		         metadata.getClusterName());
		   for ( Host host : metadata.getAllHosts() ) {
		      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
		         host.getDatacenter(), host.getAddress(), host.getRack());
		   }
		
		ResultSet result = session.execute(query);
		List<Row> rows =result.all();
	
		BufferedWriter bw=null;
		FileWriter fw = null;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		String t = dateFormat.format(date).replace("/", "-").replace(":", "-").replaceAll("\\s+", "T");
		
		
		for (Row r: rows) {
			String username = r.getString("username");
			String password = r.getString("password");
			System.out.println(username);
			System.out.println(password);
			
			File file = new File("C:/Users/Mahender/Documents/cassandraOutput/"+t+".txt");
			if (!file.exists()) {
				file.createNewFile();	
			}
			 fw = new FileWriter(file.getAbsoluteFile());
			 bw =  new BufferedWriter(fw);
			 bw.write(rows.toString());
			 bw.close();
			 LOG.info("Data is inserted into insertperson table and flatfile is created in cassandraOutput folder!!");
			

			}
		
		MappingManager manager =  new MappingManager(session);
		Mapper<dupperson> mapper = manager.mapper(dupperson.class);
		dupperson person = mapper.get("1");
		System.out.println(person.getAge());
		System.out.println(person.getName());
		System.out.println(person.getPassword());
		System.out.println(person.getUsername());
		
		
		
		
	/*Mapper<dupperson> manager =  new MappingManager((Session) session.getCluster()).mapper(dupperson.class);
		dupperson dupPerson = manager.get("1");
		System.out.println(dupPerson.getAge());
*/		
		cluster.close();
		
	}


}
