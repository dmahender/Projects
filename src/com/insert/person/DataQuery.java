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
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class DataQuery {
	 private static final Logger LOG = LoggerFactory.getLogger(DataQuery.class);
	 CassandraConfig config =  new CassandraConfig();
	
	 
	 
	/*This Method retrieves data from dupperson
	 * 
	 * 
	 * */
		public void selectData() {
			String selectQuery = "select * from dupperson";
			ResultSet results = config.getSession().execute(selectQuery);
			for(Row row: results){
			LOG.info("ID:  "+row.getString("id"));
			LOG.info("Age: "+row.getInt(1));
			}
			LOG.info("");
			config.getSession().close();
			}
		
		
		
		
		/*This method gets the data from dupperson table and inserts into intertperson table 
		 * and creates a flat file in C:/Users/Mahender/Documents/cassandraOutput
		 * 
		 * */
		
		public void  createFileAndInsertDB() throws IOException {
 
			BufferedWriter bw=null;
			FileWriter fw = null;
			Row row = null;
			String query = "SELECT * FROM dupperson";
			ResultSet result = config.getSession().execute(query);
			Iterator<Row> itr = result.iterator();
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			String t = dateFormat.format(date).replace("/", "-").replace(":", "-").replaceAll("\\s+", "T");
			ArrayList<String> listRow = new ArrayList<String>();
			
			while (itr.hasNext()) {
			row = (Row) itr.next();
			LOG.info(row+"list of data from cassandra");
			listRow.add(row.toString());
			PreparedStatement ps =config.getSession().prepare("insert into InsertPerson(id,age,name,password,username) values (?,?,?,?,?)");
			config.getSession().execute(ps.bind(row.getString(0),row.getInt(1),row.getString(2),row.getString(3),row.getString(4)));
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
			config.getSession().close();		
			
			}	
		
		
		/*This Method retrieves data from dupperson and created a flat file in C:/Users/Mahender/Documents/cassandraOutput.
		 *  The object mapping is done and data is obtained from model class.*/
		
		
		public void CreateFileAndMapping() throws IOException {
			
			BufferedWriter bw=null;
			FileWriter fw = null;
			String query = "SELECT * FROM dupperson";
			ResultSet result = config.getSession().execute(query);
			List<Row> rows =result.all();
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			String t = dateFormat.format(date).replace("/", "-").replace(":", "-").replaceAll("\\s+", "T");
			
			for (Row r: rows) {
			LOG.info("ID:  "+r.getString("id"));
			LOG.info("Age: "+r.getInt(1));
			File file = new File("C:/Users/Mahender/Documents/cassandraOutput/"+t+".txt");
			if (!file.exists()) {
			file.createNewFile();	
			}
			fw = new FileWriter(file.getAbsoluteFile());
			bw =  new BufferedWriter(fw);
			bw.write(rows.toString());
			bw.close();
			}
			
			MappingManager manager =  new MappingManager(config.getSession());
			Mapper<dupperson> mapper = manager.mapper(dupperson.class);
			dupperson person = mapper.get("1");
			LOG.info("ID: ", person.getId());
			LOG.info("Age: ", person.getAge());
			LOG.info("Name: ", person.getName());
			config.getSession().close();
			}
		
			}
		
	
