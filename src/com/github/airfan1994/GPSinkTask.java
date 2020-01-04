package com.github.airfan1994;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
public class GPSinkTask extends SinkTask{
	String gpURL;
	String gpUser;
	String gpPwd;
	Connection conn;
	Statement stmt;
	
	public Connection getGPConncetion() throws Exception {
		String msDriverName = "org.postgresql.Driver";
		Class.forName(msDriverName).newInstance();
		DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
		conn = DriverManager.getConnection(gpURL, gpUser, gpPwd);
		return conn;
	}

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(Collection<SinkRecord> arg0) {
		for(SinkRecord record: arg0) {
			System.out.println(record.value());
			try {
				stmt.execute(record.value().toString());
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void start(Map<String, String> arg0) {
		// TODO Auto-generated method 
		this.gpURL = arg0.get("gpURL");
		this.gpUser = arg0.get("gpUser");
		this.gpPwd = arg0.get("gpPWd");
		try {
			getGPConncetion();
			stmt = conn.createStatement();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		try {
			stmt.close();
			conn.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
