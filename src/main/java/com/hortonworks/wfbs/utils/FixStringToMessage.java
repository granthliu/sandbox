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

import quickfix.FieldNotFound;
import quickfix.InvalidMessage;
import quickfix.Message;

/**
 * This is a very quick and dirty hack to convert from fix to an object we can handle
 */

public class FixStringToMessage {

    public static final Message convert(String fixStr) {
	    Message msg = null;
	    try {
			msg = new Message(fixStr);
		} catch (InvalidMessage e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return msg;
		//MessageUtils.parse(new DefaultMessageFactory(), new DataDictionary(), this.fixstr);
    }

    
    //Some minor exercise
	public static void main(String[] args) {
		String input = "8=FIX.4.49=143935=R34=15949=SENDERCOMPID152=20131216-13:37:16.30256=WFS_XYZ_TEST_12345_DLRDPL131=TRD_20131216_WFS_TRSY_1_4_1146=155=[N/A]60=20131216-13:37:1675=20131216464=Y5745=1828=575766=BMKSWITCH453=3448=wfscust13447=C452=3802=4523=Well Customer13803=2523=NY803=25523=US803=4000523=94039005803=4001448=Wells Fargo Test Customer447=C452=1802=2523=DJ10803=4002523=NO803=4003448=Bilateral447=C452=4555=2600=         1     11/30/19 7yr602=912828UB4603=1607=6609=TNOTE611=20191130249=20121130615=1624=1556=USD588=20121220686=1685=25720000824=TRD_TRSY_1566=98.484375600=7 Yr vs 3M LIBOR602=RU00007YL3MS603=8607=12609=IRSUSD764=BMK611=20191221624=P556=USD588=20121221677=LIBOR678=3M686=6685=25000000824=TRD_XYZ_1566=1.32581539=1524=Bilateral525=C538=410=141";
		Message fixMsg = FixStringToMessage.convert(input);
		System.out.println(fixMsg.toXML());
		try {
			System.out.println(fixMsg.getString(60));
			System.out.println(fixMsg.getHeader().getString(8));
		} catch (FieldNotFound e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
