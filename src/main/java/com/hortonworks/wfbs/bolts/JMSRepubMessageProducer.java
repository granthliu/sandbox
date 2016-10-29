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

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;

import com.hortonworks.wfbs.spouts.WFBTupleProducer;

import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.tuple.Tuple;

public class JMSRepubMessageProducer implements JmsMessageProducer {
	private static final long serialVersionUID = 4747562726130925478L;
	private static Logger logger = Logger.getLogger(JmsMessageProducer.class);

	@Override
    public Message toMessage(Session session, Tuple input) throws JMSException{
		String payload = input.getStringByField(NormalizationBolt.XML_FN);
		if (payload == null || payload.trim().length() == 0) {
			payload = input.getStringByField(NormalizationBolt.RAW_FN);
		}
		logger.debug("Sending JMS Message:" + input.getString(0));
		//TODO: Repub only the XML (or raw in case of XML messages)
        TextMessage tm = session.createTextMessage(payload);
        //Leaving this here, but apparently this has no effect. We must set at producer level.
        tm.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        return tm;
    }
}
