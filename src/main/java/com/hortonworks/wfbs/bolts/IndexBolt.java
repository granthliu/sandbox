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

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hortonworks.wfbs.utils.IndexClient;
import com.hortonworks.wfbs.utils.IndexableFIXTransaction;
import com.hortonworks.wfbs.utils.IndexableTXTXMLTransaction;
import com.hortonworks.wfbs.utils.IndexableTransaction;


/**
 * Index normalized data. Broken out from other tasks for easy scalability.
 * 
 * @author gliu
 * 
 */
public class IndexBolt implements IRichBolt {
	private static final long serialVersionUID = 6711927509800236001L;
	OutputCollector collector;
	private static Logger logger = Logger.getLogger(IndexBolt.class);

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		logger.debug("Input to index:" + input.getString(0));
		IndexableTransaction msg;
		if (input.getSourceStreamId().equals(NormalizationBolt.FIX_STREAM)) {
			msg = new IndexableFIXTransaction(input.getStringByField(NormalizationBolt.ID_FN), 
					input.getStringByField(NormalizationBolt.TAG35_FN), 
					input.getStringByField(NormalizationBolt.TAG49_FN), 
					input.getStringByField(NormalizationBolt.TAG52_FN), 
					input.getStringByField(NormalizationBolt.TAG60_FN), 
					input.getStringByField(NormalizationBolt.TAG75_FN), 
					input.getStringByField(NormalizationBolt.TAG600_FN), 
					input.getStringByField(NormalizationBolt.TAG609_FN));
					try {
						IndexClient.getInstance().index(msg, NormalizationBolt.FIX_STREAM.toLowerCase());
					} catch (JsonProcessingException e) {
						logger.error("Failed to index:" + msg);
						e.printStackTrace();
					}
		} else {
			msg = new IndexableTXTXMLTransaction(input.getStringByField(NormalizationBolt.TRADEID_FN), 
					input.getStringByField(NormalizationBolt.SOURCEID_FN), 
					input.getStringByField(NormalizationBolt.CONTENTTYPE_FN), 
					input.getStringByField(NormalizationBolt.ID_FN),
					input.getStringByField(NormalizationBolt.EFFECTIVE_DATE_FN),
					input.getStringByField(NormalizationBolt.TERMINATION_DATE_FN));
					try {
						IndexClient.getInstance().index(msg, NormalizationBolt.XML_TXT_STREAM.toLowerCase());
					} catch (JsonProcessingException e) {
						logger.error("Failed to index:" + msg);
						e.printStackTrace();
					}
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("response"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}