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
package com.hortonworks.wfbs.spouts;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import backtype.storm.contrib.jms.JmsTupleProducer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * A simple <code>JmsTupleProducer</code> that expects to receive
 * JMS <code>Map</code> objects with a unique id, filename, and raw message.
 * Falls back to just treating the entire thing like a string though.
 * <p/>
 * Ouputs a tuple with corresponding fields
 * <p/>
 */
@SuppressWarnings("serial")
public class WFBTupleProducer implements JmsTupleProducer {
	public static final String GUID = "guid";
	public static final String FILENAME = "filename";
	public static final String TYPE = "type";
	public static final String RAW = "raw_message";
	public static final String IN_JMS_STREAM = "JMS";
	public static final String EFFECTIVE_DATE_FN = "effective_date";
	public static final String TERMINATION_DATE_FN = "termination_date";
	
	public Values toTuple(Message msg) throws JMSException {
		if (msg instanceof MapMessage) {
			MapMessage mapmsg = (MapMessage) msg;
			String type = mapmsg.getString(TYPE);
			return new Values(mapmsg.getString(GUID), mapmsg.getString(FILENAME),type ,  
					compressed(type) ? mapmsg.getBytes(RAW) : mapmsg.getString(RAW) ,
							mapmsg.getString(EFFECTIVE_DATE_FN), mapmsg.getString(TERMINATION_DATE_FN));
		} else {
			String payload = ((TextMessage) msg).getText();
			return new Values(payload);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(IN_JMS_STREAM, new Fields(GUID, FILENAME, TYPE, RAW, EFFECTIVE_DATE_FN, TERMINATION_DATE_FN));
	}
	
	public static boolean compressed(String type) {
		return type.startsWith("COMP_");
	}


}
