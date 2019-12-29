package com.github.airfan1994;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
					records.add(makeSR(key, updateBuffer.toString(), lastSyncVersion))
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

}
