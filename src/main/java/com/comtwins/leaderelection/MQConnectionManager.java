package com.comtwins.leaderelection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;


public class MQConnectionManager {
	private static final Logger logger = LoggerFactory.getLogger(MQConnectionManager.class);
	protected static Connection connection = null;

	private MQConnectionManager(){}
	
	public static synchronized Connection getConnection() throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException, IOException, TimeoutException {
		if (connection == null){
			logger.info("establishing a connection to RabbitMQ server");
			String mqURI = System.getProperty("rabbitmq.URI", "amqp://localhost:5672");

			ConnectionFactory factory = new ConnectionFactory();
			factory.setUri(mqURI);
			factory.setAutomaticRecoveryEnabled(true);
				
			connection = factory.newConnection();
			connection.addShutdownListener(new ShutdownListener(){
				@Override
				public void shutdownCompleted(ShutdownSignalException e) {
					logger.info("The connection to RabbitMQ server has been shutdown now", e);
					if (e.isInitiatedByApplication()){
						connection = null;
					}
				}
			});
		}
		return connection;
	}
}
