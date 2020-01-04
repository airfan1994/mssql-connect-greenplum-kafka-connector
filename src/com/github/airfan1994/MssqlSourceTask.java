package com.github.airfan1994;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.io.*;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class MssqlSourceTask extends SourceTask{
	String dbURL;
	String userName;
	String userPwd;
	String tableName;
	String[] colName;
	int[] colType;
	String primaryKey;
	int timestamp = 1000;
	Connection dbConn;
	Statement stmt;
	String topic;
	long lastSyncVersion = 0L;
	String gpTable;
	int primaryType;
	
	public Connection getMssqlConncetion() throws Exception {
		String msDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		Class.forName(msDriverName).newInstance();
		DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
		dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
		return dbConn;
	}
	
	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<SourceRecord> poll() throws InterruptedException {
		ArrayList<SourceRecord> records = new ArrayList();
		try {
			long currentVersion =  getCurrentVersion();
			ResultSet res = stmt.executeQuery("select ct.sys_change_operation, ct." + primaryKey +", em.* from changetable (changes[" + tableName + "], " + lastSyncVersion + ") as ct left join " + tableName + " em on ct." + primaryKey +" = em." + primaryKey);
			StringBuffer insertPrefix = new StringBuffer("insert into " + gpTable + " values(");
			StringBuffer updatePrefix = new StringBuffer("update " + gpTable + " set ");
			while (res.next()) {
				String changetype = res.getString(1);
				String key = res.getString(2);
				if (changetype.equals("D")) {
					String query = "delete from " + gpTable +" " + constructPrimaryQuery(key);
					records.add(makeSR(key, query, lastSyncVersion));
				}
				else if (changetype.equals("I")) {
					StringBuffer valBuffer = getValFromRes(res);
					StringBuffer insertBuffer = new StringBuffer();
					insertBuffer.append(insertPrefix);
					insertBuffer.append(valBuffer);
					insertBuffer.append(")");
					records.add(makeSR(key, "delete from " + gpTable +" " + constructPrimaryQuery(key), lastSyncVersion));
					records.add(makeSR(key, insertBuffer.toString(), lastSyncVersion));
				}
				else if (changetype.equals("U")) {
					StringBuffer updateBuffer = new StringBuffer();
					updateBuffer.append(updatePrefix);
					for(int i = 1; i < colName.length; i++) {
						if(i == 1) {
							updateBuffer.append(",");
						}
						updateBuffer.append(colName[i]);
						updateBuffer.append("=");
						if(colType[i] == 1) {
							updateBuffer.append("'");
							updateBuffer.append(res.getString(i + 3));
							updateBuffer.append("'");
						}
						else {
							updateBuffer.append(res.getString(i + 3));
						}
					}
					updateBuffer.append(")");
					updateBuffer.append(constructPrimaryQuery(key));
					records.add(makeSR(key, updateBuffer.toString(), lastSyncVersion));
				}
				else {
					throw new Exception("unsupported CT log.");
				}
				System.out.println(records.get(records.size() - 1).value());
			}
			lastSyncVersion = currentVersion;
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return records;
	}

	@Override
	public void start(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		this.dbURL = arg0.get("dbURL");
		this.userName = arg0.get("userName");
		this.userPwd = arg0.get("userPwd");
		this.tableName = arg0.get("tableName");
		this.primaryKey = arg0.get("primaryKey");
		try {
			dbConn = getMssqlConncetion();
			String configFile = arg0.get("configFile");
			parsePars(configFile);
			parsePrimaryType(arg0.get("primaryTypeStr"));
			stmt = dbConn.createStatement();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		this.topic = arg0.get("topic");
		this.gpTable = arg0.get("gpTable");
	}

	@Override
	public void stop() {
		try {
			stmt.close();
			dbConn.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void parsePars(String configFile) throws Exception{
		HashSet<String> numType = new HashSet<String>(Arrays.asList("tinyint,smallint,int,bigint,numeric,demcimal,float,real".split(",")));
		HashSet<String> strType = new HashSet<String>(Arrays.asList("char,varchar,string,text,nchar,nvarchar,ntext,date,time,datetime,datetime2,smalldatetime,datetimeoffset".split(",")));
		BufferedReader br = new BufferedReader(new FileReader(configFile));
		String line;
		int i = 0;
		while((line = br.readLine()) != null) {
			String[] parts = line.split(",");
			if (colName == null) {
				colName = new String[parts.length];
				colType = new int[parts.length];
			}
			colName[i] = parts[0];
			if(numType.contains(parts[1])) {
				colType[i] = 0;
			}
			else if(strType.contains(parts[0])) {
				colType[i] = 1;
			}
			else{
				throw new Exception("unsupported data type");
			}
			i++;
		}
		br.close();
	}
	public void parsePrimaryType(String primaryTypeStr) throws Exception {
		HashSet<String> numType = new HashSet<String>(Arrays.asList("tinyint,smallint,int,bigint,numeric,demcimal,float,real".split(",")));
		HashSet<String> strType = new HashSet<String>(Arrays.asList("char,varchar,string,text,nchar,nvarchar,ntext,date,time,datetime,datetime2,smalldatetime,datetimeoffset".split(",")));
		if(numType.contains(primaryTypeStr)) {
			this.primaryType = 0;
		}
		else if(strType.contains(primaryTypeStr)) {
			this.primaryType = 1;
		}
		else {
			throw new Exception("unsupported data type");
		}
	}
	public long getCurrentVersion() throws Exception {
		Statement stmt = dbConn.createStatement();
		ResultSet res = stmt.executeQuery("select CHANGE_TRACKING_CURRENT_VERSION()");
		res.next();
		return res.getLong(1);
	}
	
	public SourceRecord makeSR(String key, String value, long version) {
		Map<String, Object> sourcePartition = Collections.singletonMap("filename", "sqlserver");
		Map<String, Object> sourceOffset = Collections.singletonMap("position", version + "_" + key);
		return new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, value);
	}
	
	public String constructPrimaryQuery(String key) {
		if (primaryType == 0) {
			return "where" + primaryKey + "=" + key;
		}
		else {
			return "where" + primaryKey + "='" + key + "'";
		}
	}
	public StringBuffer getValFromRes(ResultSet res) throws Exception {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < colName.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			if (colType[i] == 1) {
				sb.append("'");
				sb.append(res.getString(i + 3));
				sb.append("'");
			}
			else {
				sb.append(res.getString(i + 3));
			}
		}
		return sb;
			
	}
}
