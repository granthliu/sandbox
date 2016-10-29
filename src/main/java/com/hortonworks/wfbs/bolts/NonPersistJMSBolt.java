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
package com.hortonworks.wfbs.bolts;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import backtype.storm.topology.base.BaseRichBolt;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 * JMS bolt isn't written for easy extensibility. Copy entire thing here 
 * so it can be run in non persistent mode.
 *
 */
public class NonPersistJMSBolt extends BaseRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(NonPersistJMSBolt.class);
	
	private boolean autoAck = true;
	
	// javax.jms objects
	private Connection connection;
	private Session session;
	private MessageProducer messageProducer;
	
	// JMS options
	private boolean jmsTransactional = false;
	private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;
	
	
	private JmsProvider jmsProvider;
	private JmsMessageProducer producer;
	
	
	private OutputCollector collector;
	
	/**
	 * Set the JmsProvider used to connect to the JMS destination topic/queue
	 * @param provider
	 */
	public void setJmsProvider(JmsProvider provider){
		this.jmsProvider = provider;
	}
	
	/**
	 * Set the JmsMessageProducer used to convert tuples
	 * into JMS messages.
	 * 
	 * @param producer
	 */
	public void setJmsMessageProducer(JmsMessageProducer producer){
		this.producer = producer;
	}
	
	/**
	 * Sets the JMS acknowledgement mode for JMS messages sent
	 * by this bolt.
	 * <p/>
	 * Possible values:
	 * <ul>
	 * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
	 * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
	 * </ul>
	 * @param acknowledgeMode (constant defined in javax.jms.Session)
	 */
	public void setJmsAcknowledgeMode(int acknowledgeMode){
		this.jmsAcknowledgeMode = acknowledgeMode;
	}
	
	/**
	 * Set the JMS transactional setting for the JMS session.
	 * 
	 * @param transactional
	 */
//	public void setJmsTransactional(boolean transactional){
//		this.jmsTransactional = transactional;
//	}
	
	/**
	 * Sets whether or not tuples should be acknowledged by this
	 * bolt.
	 * <p/>
	 * @param autoAck
	 */
	public void setAutoAck(boolean autoAck){
		this.autoAck = autoAck;
	}


	/**
	 * Consumes a tuple and sends a JMS message.
	 * <p/>
	 * If autoAck is true, the tuple will be acknowledged
	 * after the message is sent.
	 * <p/>
	 * If JMS sending fails, the tuple will be failed.
	 */
	@Override
	public void execute(Tuple input) {
		// write the tuple to a JMS destination...
		LOG.debug("Tuple received. Sending JMS message.");
		
		try {
			Message msg = this.producer.toMessage(this.session, input);
			if(msg != null){
				if (msg.getJMSDestination() != null) {
					this.messageProducer.send(msg.getJMSDestination(), msg);
				} else {
					this.messageProducer.send(msg);
				}
			}
			if(this.autoAck){
				//LOG.debug("ACKing tuple: " + input);
				this.collector.ack(input);
			}		
		} catch (JMSException e) {
			// failed to send the JMS message, fail the tuple fast
			LOG.warn("Failing tuple: " + input);
			LOG.warn("Exception: ", e);
			this.collector.fail(input);
		}
	}

	/**
	 * Releases JMS resources.
	 */
	@Override
	public void cleanup() {
		try {
			LOG.debug("Closing JMS connection.");
			this.session.close();
			this.connection.close();
		} catch (JMSException e) {
			LOG.warn("Error closing JMS connection.", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

    /**
	 * Initializes JMS resources.
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		if(this.jmsProvider == null || this.producer == null){
			throw new IllegalStateException("JMS Provider and MessageProducer not set.");
		}
		this.collector = collector;
		LOG.debug("Connecting JMS..");
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

			
			this.session = connection.createSession(this.jmsTransactional,
					this.jmsAcknowledgeMode);
			this.messageProducer = session.createProducer(dest);
			this.messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			connection.start();
		} catch (Exception e) {
			LOG.warn("Error creating JMS connection.", e);
		}	
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
}
