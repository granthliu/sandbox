
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



/**
 * Fix fields to index
 * @author gliu
 * 
 */
public class IndexableFIXTransaction  implements IndexableTransaction {
	private String ID;
	private String TAG35;
	private String TAG49;
	private String TAG52;
	private String TAG60;
	private String TAG75;
	private String TAG600;
	private String TAG609;
	
	public String getTAG35() {
		return TAG35;
	}

	public void setTAG35(String tAG35) {
		TAG35 = tAG35;
	}

	public String getTAG49() {
		return TAG49;
	}

	public void setTAG49(String tAG49) {
		TAG49 = tAG49;
	}

	public String getTAG52() {
		return TAG52;
	}

	public void setTAG52(String tAG52) {
		TAG52 = tAG52;
	}

	public String getTAG60() {
		return TAG60;
	}

	public void setTAG60(String tAG60) {
		TAG60 = tAG60;
	}

	public String getTAG75() {
		return TAG75;
	}

	public void setTAG75(String tAG75) {
		TAG75 = tAG75;
	}

	public String getTAG600() {
		return TAG600;
	}

	public void setTAG600(String tAG600) {
		TAG600 = tAG600;
	}

	public String getTAG609() {
		return TAG609;
	}

	public void setTAG609(String tAG609) {
		TAG609 = tAG609;
	}

	public IndexableFIXTransaction(String id, String TAG35, String TAG49, String TAG52,String TAG60, 
			String TAG75 , String TAG600, String TAG609) {
		super();
		this.ID = id;
		this.TAG35 = TAG35;
		this.TAG49 = TAG49;
		this.TAG52 = TAG52;
		this.TAG60 = TAG60;
		this.TAG75 = TAG75;
		this.TAG600 = TAG600;
		this.TAG609 = TAG609;
	}

	public String getID() {
		return ID;
	}

	public void setID(String iD) {
		ID = iD;
	}


	
}