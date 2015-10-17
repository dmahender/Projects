package com.insert.person;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.spring.cass.CassandraApp;


public class InsertData {
	
	private static Cluster cluster;
	 private static final Logger LOG = LoggerFactory.getLogger(InsertData.class);

	public static void main(String[] args) throws IOException {

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		
		Session session = cluster.connect("demo");
		
		String query = "SELECT * FROM dupperson";
		
		ResultSet result = session.execute(query);
		
		Iterator<Row> itr = result.iterator();
		
		dupperson dupPerson = new dupperson();
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		
		Date date = new Date();
		
		String t = dateFormat.format(date).replace("/", "-").replace(":", "-").replaceAll("\\s+", "T");
		
		BufferedWriter bw=null;
		
		FileWriter fw = null;
		
		Row row = null;
		
		ArrayList<String> listRow = new ArrayList<String>();
		
		while (itr.hasNext()) {
			row = (Row) itr.next();
			LOG.info(row+"list of data from cassandra");
			
		listRow.add(row.toString());
		
		PreparedStatement ps = session.prepare("insert into InsertPerson(id,age,name,password,username) values (?,?,?,?,?)");
		session.execute(ps.bind(row.getString(0),row.getInt(1),row.getString(2),row.getString(3),row.getString(4)));
		}
			
			File file = new File("C:/Users/Mahender/Documents/cassandraOutput/"+t+".txt");
			if (!file.exists()) {
				file.createNewFile();	
			}
			 fw = new FileWriter(file.getAbsoluteFile());
			 bw =  new BufferedWriter(fw);
			 bw.write(listRow.toString());
			 bw.close();
			 LOG.info("Data is inserted into insertperson table and flatfile is created in cassandraOutput folder!!");
			
			
			 cluster.close();
	}	
	
				
	}	
		
		/*String query = "SELECT * FROM dupperson";
		ResultSet result = session.execute(query);
		//System.out.println(result.all());
		
		
		Iterator<Row> itr = result.iterator();
		while (itr.hasNext()) {
			person person = new person();
			Row row = (Row) itr.next();
			System.out.println(row);
			//System.out.println(itr.next());
			PreparedStatement ps = session.prepare("insert into InsertPerson(id,age,name,password,username) values (?,?,?,?,?)");
			session.execute(ps.bind(row.getString(0),row.getInt(1),row.getString(2),row.getString(3),row.getString(4)));*/
		



