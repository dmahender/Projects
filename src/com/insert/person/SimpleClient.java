package com.insert.person;

import java.io.IOException;


public class SimpleClient extends DataQuery {

	private static CassandraConfig config =  new CassandraConfig();
	private static DataQuery dataQuery = new DataQuery();


	public static void main(String[] args) throws IOException {
		
		config.connect("127.0.0.1");
		//dataQuery.selectData();
		dataQuery.CreateFileAndMapping();
		//dataQuery.createFileAndInsertDB();
		config.close();
	}


	
}
