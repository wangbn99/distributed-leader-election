package com.comtwins.leaderelection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.rabbitmq.client.AMQP.Queue.DeclareOk;

@SuppressWarnings("InfiniteLoopStatement")
public class LeaderElectionImpl extends Thread implements LeaderElection {

	public static final String exchangeName = "exchange.leader";
	public static final String exchangeType = "topic";
	public static final String deadLetteredQueue = "queue.deadlettered";
	public static final String deadLetteredBindKey = "key.bind.deadlettered";
	public static final String deadLetteredRoutingKey = "key.routing.deadlettered";
	public static final String leaderQueue = "queue.leader";
	public static final String leaderBindKey = "key.bind.leader";
	public static final String leaderLockQueue = "queue.leader.lock";
	public static final String leaderRoutingKey = "key.routing.leader";
	public static final String leaderToken = "PEBBLE";

	private static final Logger logger = LoggerFactory.getLogger(LeaderElectionImpl.class);

	private static boolean isLeader = false;
	private Channel channel = null;
	private long leaderDeliveryTag = 0L;
	private LeaderService leaderService = null;

	public static boolean isLeader() {
		return isLeader;
	}

	@Override
	public LeaderService getLeaderService() {
		return leaderService;
	}

	@Override
	public void setLeaderService(LeaderService leaderService) {
		this.leaderService = leaderService;
	}

	@Override
	public void run() {
		while (true) {
			electLeader();
			try {
				TimeUnit.MILLISECONDS.sleep(500L + new Random().nextInt(500));
			} catch (InterruptedException ignored) {
			}
		}
	}

	@Override
	public void startLeaderElection() {
		logger.info("starting leader election");
		this.start();
	}

	@Override
	public void stopLeader() {
		stopLeaderService();
		if (isLeader) {
			isLeader = false;
		}
		if (channel != null && channel.isOpen()) {
			try {
				channel.basicReject(leaderDeliveryTag, true);
			} catch (IOException ignored) {
			}
			try {
				channel.close();
			} catch (IOException | TimeoutException ignored) {
			}
		}
		channel = null;
		logger.info("leader stopped");
	}

	private void electLeader() {
		try {
			Connection connection = MQConnectionManager.getConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(exchangeName, exchangeType,true);
			channel.queueDeclare(deadLetteredQueue, true, false, false, null);
			channel.queueBind(deadLetteredQueue, exchangeName, deadLetteredBindKey);
			
			Map<String, Object> args = new HashMap<>();
			args.put("x-max-length", 1);
			args.put("x-dead-letter-exchange", exchangeName);
			args.put("x-dead-letter-routing-key", deadLetteredRoutingKey);
			channel.queueDeclare(leaderQueue, true, false, false, args);
			channel.queueBind(leaderQueue, exchangeName, leaderBindKey);

			DeclareOk ok = channel.queueDeclarePassive(deadLetteredQueue);
			if (ok.getMessageCount() < 1){
				try{
					channel.queueDeclarePassive(leaderLockQueue);
				} catch (Exception e){
					if (!channel.isOpen()){
						channel = connection.createChannel();
					}
				}
				channel.queueDeclare(leaderLockQueue, false, true, true, null);
				while (true){
					ok = channel.queueDeclarePassive(deadLetteredQueue);
					if (ok.getMessageCount() < 1){
						logger.info("publish leader token to leader queue");
						channel.basicPublish(exchangeName, leaderRoutingKey,
								MessageProperties.PERSISTENT_TEXT_PLAIN, leaderToken.getBytes());
					} else {
						break;
					}
				}
				channel.queueDelete(leaderLockQueue);
			}

			DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

				if ( !leaderToken.equals(message.trim()) ) {
					logger.info("there is non leader token in leader queue: " + message);
					logger.info("republish leader token to leader queue");
					channel.basicPublish(exchangeName, leaderRoutingKey,
							MessageProperties.PERSISTENT_TEXT_PLAIN, leaderToken.getBytes());
					channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
				} else {
					logger.info("won the leader election, I'm the leader now");
					isLeader = true;
					leaderDeliveryTag = delivery.getEnvelope().getDeliveryTag();
					leaderService.startService();
				}

			};

			channel.basicQos(1);
			channel.basicConsume(leaderQueue, false, deliverCallback, consumerTag -> {});

		} catch (Exception e) {
			logger.error("Leader election failed", e);
		}
		stopLeader();
	}

	private void stopLeaderService() {
		leaderService.stopService();
	}

}
