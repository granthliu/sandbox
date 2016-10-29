/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
* 
*      http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.hortonworks.wfbs.utils;

import java.io.Serializable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.jms.JmsProvider;

import com.hortonworks.wfbs.spouts.SpringJmsProvider;

/**
 * TEST CLASS - Standalone JMS Consumer, same code as Storm JMS Bolt to test
 * performance without storm
 *
 */
@SuppressWarnings("serial")
public class WFBJMSConsumer implements MessageListener {
	// JMS options
	private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;	
	private boolean distributed = true;
	private JmsProvider jmsProvider;
	private LinkedBlockingQueue<Message> queue;
	private ConcurrentHashMap<String, Message> pendingMessages;

	private transient Connection connection;
	private transient Session session;
	
	private boolean hasFailures = false;
	public final Serializable recoveryMutex = "RECOVERY_MUTEX";
	private Timer recoveryTimer = null;
	private long recoveryPeriod = -1; // default to disabled
	
	/**
	 * Sets the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
	 * <p/>
	 * Possible values:
	 * <ul>
	 * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
	 * </ul>
	 * @param mode JMS Session Acknowledgement mode
	 * @throws IllegalArgumentException if the mode is not recognized.
	 */
	public void setJmsAcknowledgeMode(int mode){
		switch (mode) {
		case Session.AUTO_ACKNOWLEDGE:
		case Session.CLIENT_ACKNOWLEDGE:
		case Session.DUPS_OK_ACKNOWLEDGE:
			break;
		default:
			throw new IllegalArgumentException("Unknown Acknowledge mode: " + mode + " (See javax.jms.Session for valid values)");

		}
		this.jmsAcknowledgeMode = mode;
	}
	
	/**
	 * Returns the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
	 * @return
	 */
	public int getJmsAcknowledgeMode(){
		return this.jmsAcknowledgeMode;
	}
	
	/**
	 * Set the <code>backtype.storm.contrib.jms.JmsProvider</code>
	 * implementation that this Spout will use to connect to 
	 * a JMS <code>javax.jms.Desination</code>
	 * 
	 * @param provider
	 */
	public void setJmsProvider(JmsProvider provider){
		this.jmsProvider = provider;
	}

	/**
	 * <code>javax.jms.MessageListener</code> implementation.
	 * <p/>
	 * Stored the JMS message in an internal queue for processing
	 * by the <code>nextTuple()</code> method.
	 */
	public void onMessage(Message msg) {

		this.queue.offer(msg);
	}

	boolean jms_sessionasynch = false;
	boolean jms_optimizeack = true;
	int jms_prefetch = 15000;
	int jms_maxthreadpoolsize = 1000;
	boolean jms_usecompression = true;
	public void setOptions(boolean jms_sessionasynch, boolean jms_optimizeack, int jms_prefetch, int jms_maxthreadpoolsize, boolean jms_usecompression) {
		this.jms_sessionasynch = jms_sessionasynch;
		this.jms_optimizeack = jms_optimizeack;
		this.jms_prefetch = jms_prefetch;
		this.jms_maxthreadpoolsize = jms_maxthreadpoolsize;		
		this.jms_usecompression = jms_usecompression;		
	}

	public void close() {
		try {
			System.err.println("Closing JMS connection.");
			this.session.close();
			this.connection.close();
		} catch (JMSException e) {
			System.err.println("Error closing JMS connection."  + e);
		}

	}

	/*
	 * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
	 */
	public void ack(Object msgId) {

		Message msg = this.pendingMessages.remove(msgId);
		if (msg != null) {
			try {
				msg.acknowledge();
				System.err.println("JMS Message acked: " + msgId);
			} catch (JMSException e) {
				System.err.println("Error acknowldging JMS message: " + msgId  + e);
			}
		} else {
			System.err.println("Couldn't acknowledge unknown JMS message ID: " + msgId);
		}

	}

	/*
	 * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
	 */
	public void fail(Object msgId) {
		System.err.println("Message failed: " + msgId);
		this.pendingMessages.remove(msgId);
		synchronized(this.recoveryMutex){
		    this.hasFailures = true;
		}
	}


	/**
         * Returns <code>true</code> if the spout has received failures 
         * from which it has not yet recovered.
         */
	public boolean hasFailures(){
	        return this.hasFailures;
	}
	
	protected void recovered(){
	        this.hasFailures = false;
	}
	
	/**
	 * Sets the periodicity of the timer task that 
	 * checks for failures and recovers the JMS session.
	 * 
	 * @param period
	 */
	public void setRecoveryPeriod(long period){
	    this.recoveryPeriod = period;
	}
	
	public boolean isDistributed() {
		return this.distributed;
	}
	
	/**
	 * Sets the "distributed" mode of this spout.
	 * <p/>
	 * If <code>true</code> multiple instances of this spout <i>may</i> be
	 * created across the cluster (depending on the "parallelism_hint" in the topology configuration).
	 * <p/>
	 * Setting this value to <code>false</code> essentially means this spout will run as a singleton 
	 * within the cluster ("parallelism_hint" will be ignored).
	 * <p/>
	 * In general, this should be set to <code>false</code> if the underlying JMS destination is a 
	 * topic, and <code>true</code> if it is a JMS queue.
	 * 
	 * @param distributed
	 */
	public void setDistributed(boolean distributed){
		this.distributed = distributed;
	}


	private static final String toDeliveryModeString(int deliveryMode) {
		switch (deliveryMode) {
		case Session.AUTO_ACKNOWLEDGE:
			return "AUTO_ACKNOWLEDGE";
		case Session.CLIENT_ACKNOWLEDGE:
			return "CLIENT_ACKNOWLEDGE";
		case Session.DUPS_OK_ACKNOWLEDGE:
			return "DUPS_OK_ACKNOWLEDGE";
		default:
			return "UNKNOWN";

		}
	}
	
	protected Session getSession(){
	        return this.session;
	}
	
	private boolean isDurableSubscription(){
	    return (this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE);
	}
	
	
	private class RecoveryTask extends TimerTask {
	    private final Logger LOG = LoggerFactory.getLogger(RecoveryTask.class);

	    public void run() {
	        synchronized (WFBJMSConsumer.this.recoveryMutex) {
	            if (WFBJMSConsumer.this.hasFailures()) {
	                try {
	                    LOG.info("Recovering from a message failure.");
	                    WFBJMSConsumer.this.getSession().recover();
	                    WFBJMSConsumer.this.recovered();
	                } catch (JMSException e) {
	                    System.err.println("Could not recover jms session." + e);
	                }
	            }
	        }
	    }

	}
	
	/**
	 * <code>ISpout</code> implementation.
	 * <p/>
	 * Connects the JMS spout to the configured JMS destination
	 * topic/queue.
	 * 
	 */
	@SuppressWarnings("rawtypes")
	public void open() {
		if(this.jmsProvider == null){
			throw new IllegalStateException("JMS provider has not been set.");
		}
		this.queue = new LinkedBlockingQueue<Message>();
		this.pendingMessages = new ConcurrentHashMap<String, Message>();
		try {

			ConnectionFactory cff = this.jmsProvider.connectionFactory();
			ActiveMQConnectionFactory cf = (ActiveMQConnectionFactory) cff;	
			cf.setUseCompression(jms_usecompression);
			cf.setUseAsyncSend(true);
			//larger prefetch so we're not throttled by session queueing
			cf.getPrefetchPolicy().setAll(jms_prefetch);
			//By default, a Consumer's session will dispatch messages to the consumer in a separate thread. 
			//If auto acknowledge, you can increase throughput by passing messages straight through the Session to the Consumer 
			cf.setAlwaysSessionAsync(jms_sessionasynch);
			
			//Decrease if getting OOM (too many threads)
			cf.setMaxThreadPoolSize(jms_maxthreadpoolsize);
			//ACK batch size is 65% of the prefetch limit for the Consumer
			cf.setOptimizeAcknowledge(jms_optimizeack);
			
			Destination dest = this.jmsProvider.destination();
			this.connection = cf.createConnection();

			//set it at every level to be safe
			((ActiveMQConnection) connection).setUseCompression(jms_usecompression);
			((ActiveMQConnection) connection).setUseAsyncSend(true);
			((ActiveMQConnection) connection).getPrefetchPolicy().setAll(jms_prefetch);
			((ActiveMQConnection) connection).setAlwaysSessionAsync(jms_sessionasynch);
			((ActiveMQConnection) connection).setMaxThreadPoolSize(jms_maxthreadpoolsize);
			((ActiveMQConnection) connection).setOptimizeAcknowledge(jms_optimizeack);

			
			this.session = connection.createSession(false,
					this.jmsAcknowledgeMode);
			MessageConsumer consumer = session.createConsumer(dest);
			consumer.setMessageListener(this);
			this.connection.start();
			if (this.isDurableSubscription() && this.recoveryPeriod > 0){
			    this.recoveryTimer = new Timer();
			    this.recoveryTimer.scheduleAtFixedRate(new RecoveryTask(), 10, this.recoveryPeriod);
			}
			int count = 0;
			System.err.println("Looping");
			while(true) {
				if(++count % 1000 == 0) {
					System.err.println("messages:" + count);					
					System.err.println("messages per second:" + count);					
				}
				Message msg = this.queue.poll();
				if (msg == null) {
					System.err.println("No message");					
				} else {
					//System.err.println("Grabbing message");
					// get the tuple from the handler
					try {
						// ack if we're not in AUTO_ACKNOWLEDGE mode, or the message requests ACKNOWLEDGE
						//System.err.println("Requested deliveryMode: " + toDeliveryModeString(msg.getJMSDeliveryMode()));
						//System.err.println("Our deliveryMode: " + toDeliveryModeString(this.jmsAcknowledgeMode));
						if (this.isDurableSubscription()
								|| (msg.getJMSDeliveryMode() != Session.AUTO_ACKNOWLEDGE)) {
							//System.err.println("+++++++++++++++++++++Requesting acks.");
							String value = msg.toString();
							//System.err.println("Message ID:" + vals  + msg.getJMSMessageID());
							//System.err.println("Message:" + value.substring(0, value.length() < 20 ? (value.length() == 0 ? 0 : value.length() - 1) : 20));
							this.pendingMessages.put(msg.getJMSMessageID(), msg);
						} else {
							//System.err.println("+++++++++++++++++++++NOT Requesting acks.");
							String value = msg.toString();
							//System.err.println("Message ID:" + vals, msg.getJMSMessageID());
							//System.err.println("Message:" + value.substring(0, value.length() < 20 ? (value.length() == 0 ? 0 : value.length() - 1) : 20));
						}
						//System.err.println("++++++++++++++++++++++++++++++++++++++++++++++Message Count" + count++);
					} catch (JMSException e) {
						System.err.println("Unable to convert JMS message: " + msg);
					}
				}	
			}
		} catch (Exception e) {
			System.err.println("Error creating JMS connection." + e);
		}

	}
	
	public static void main(String[] args) {
		int parallelPaths = 1;
		int jms_prefetch = args.length > 1 ? Integer.parseInt(args[1]) : 15000;
		int jms_maxthreadpoolsize = args.length > 2 ? Integer.parseInt(args[2]) : 1000;
		boolean jms_sessionasynch = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
		boolean jms_optimizeack = args.length > 4 ? Boolean.parseBoolean(args[4]) : true;
		System.out.println("jms_prefetch:" + jms_prefetch);
		System.out.println("jms_maxthreadpoolsize:" + jms_maxthreadpoolsize);
		System.out.println("jms_sessionasynch:" + jms_sessionasynch);
		System.out.println("jms_optimizeack:" + jms_optimizeack);
		Vector<WFBJMSConsumer> spouts = new Vector<WFBJMSConsumer>();
		for(int i = 0 ; i < parallelPaths; i++) {
			WFBJMSConsumer queueSpout = new WFBJMSConsumer();
			queueSpout.setOptions(jms_sessionasynch, jms_optimizeack, jms_prefetch, jms_maxthreadpoolsize, true);
			queueSpout.setJmsProvider(new SpringJmsProvider("jms-activemq" + (i+1) + ".xml", 
					"jmsConnectionFactory", "notificationQueue"));
			queueSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
			queueSpout.setDistributed(true); // allow multiple instances
			queueSpout.setRecoveryPeriod(60000);		
			spouts.add(queueSpout);
		}
		
		java.util.Iterator<WFBJMSConsumer> i = spouts.iterator();
		while (i.hasNext()) {
			WFBJMSConsumer consumer = i.next();
			consumer.open();
		}
	}
}
