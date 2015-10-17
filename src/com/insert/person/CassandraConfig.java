package com.insert.person;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class CassandraConfig  {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraConfig.class);
	private static Cluster cluster;
	private static Session session;
	
	/*This Method has cassandra database connection configurations*/
	
	public void connect(String node1) {
		cluster=Cluster.builder().addContactPoints(node1).build();
		Metadata metadata = cluster.getMetadata();
		System.out.println("Connected to cluster: \n"+metadata.getClusterName());;
		for (Host host: metadata.getAllHosts()) {
		System.out.println("Datatacenter: "+ host.getDatacenter()+ "\nHost: "+host.getAddress()+"\nRack: "+ host.getRack());
		}
		setSession(cluster.connect("demo"));
		}
	
		public void close() {
		cluster.close();
		}

		public Session getSession() {
		return session;
		}


		public void setSession(Session session) {
		CassandraConfig.session = cluster.connect("demo");
		}

		public static Logger getLog() {
			return LOG;
		}

	
	}
