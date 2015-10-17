package com.insert.person;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;

public class Create_Data {

	public static void main(String[] args) {
		String query1 = "INSERT INTO person(Id,age, name,password,username)"+
	"values('1',28,'jintal','basti','suraram');";
		
		try {
			Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
			Session session = cluster.connect("demo");
			session.execute(query1);
			System.out.println("Query Executed");
		} catch (QueryExecutionException e) {
			
			e.getMessage();
		}
			}
}
