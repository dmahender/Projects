package com.insert.person;

import java.util.Set;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;


@Table(keyspace="demo",name="dupperson")
public class dupperson {
	
	@PartitionKey
	private String id;
	private int age;
	private String name;
	private String password;
	private String username;

	
	
	
	public dupperson(String id, int age, String name, String password,
			String username, Set<Integer> ph) {
		super();
		this.id = id;
		this.age = age;
		this.name = name;
		this.password = password;
		this.username = username;

	}
	public dupperson() {
		// TODO Auto-generated constructor stub
	}
	
	public String getId() {
		return id;
	}
	public int getAge() {
		return age;
	}
	public String getName() {
		return name;
	}
	public String getPassword() {
		return password;
	}
	public String getUsername() {
		return username;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public void setUsername(String username) {
		this.username = username;
	}

}
