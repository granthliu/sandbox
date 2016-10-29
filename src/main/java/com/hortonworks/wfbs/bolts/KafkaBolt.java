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
import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.TextMessage;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.wfbs.utils.ConfigurationClient;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * Kafka bolt
 *
 */
public class KafkaBolt extends BaseRichBolt {
	private static Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);
	private OutputCollector collector;
	private Producer<String, String> producer;
	private String topicOut;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		LOG.debug("Tuple received. Sending Kafka message.");

		//Same settings we used in the Stream Simulator project KafkaCollector. Should really make this configurable
		Properties props = new Properties();
        props.put("metadata.broker.list", 
        		ConfigurationClient.getInstance().getProperty("kf.metadata.broker.list"));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "0");
        props.put("producer.type", "async");
        props.put("batch.num.messages", "500");
        props.put("compression.codec", "1");
        props.put("default.replication.factor", "0");
		//Same settings we used in the Stream Simulator project KafkaCollector. Should really make this configurable
		
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
        topicOut = ConfigurationClient.getInstance().getProperty("kf.notificationTopicOut");
	}
	
	@Override
	public void execute(Tuple input) {		
		LOG.debug("Tuple received. Sending Kafka message.");
		String payload = input.getStringByField(NormalizationBolt.XML_FN);
		if (payload == null || payload.trim().length() == 0) {
			payload = input.getStringByField(NormalizationBolt.RAW_FN);
		}
		//TODO: Repub only the XML (or raw in case of XML messages)
		KeyedMessage<String, String> data = 
				new KeyedMessage<String, String>(topicOut, payload);
        producer.send(data);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
	}

}
