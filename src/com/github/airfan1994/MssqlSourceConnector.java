package com.github.airfan1994;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public class MssqlSourceConnector extends SourceConnector{
	String dbURL;
	String userName;
	String userPwd;
	String tableName;
	String primaryKey;
	String topic;
	String gpTable;
	String configFile;
	String primaryTypeStr;
	
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define("dbURL", Type.STRING, null, Importance.HIGH, "dbURL")
			.define("userName", Type.STRING, null, Importance.HIGH, "userName")
			.define("userPwd", Type.STRING, null, Importance.HIGH, "userPwd")
			.define("tableName", Type.STRING, null, Importance.HIGH, "tableName")
			.define("primaryKey", Type.STRING, null, Importance.HIGH, "primaryKey")
			.define("topic", Type.STRING, null, Importance.HIGH, "topic")
			.define("gpTable", Type.STRING, null, Importance.HIGH, "gpTable")
			.define("configFile", Type.STRING, null, Importance.HIGH, "configFile")
			.define("primaryTypeStr", Type.STRING, null, Importance.HIGH, "primaryTypeStr");

	@Override
	public String version() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConfigDef config() {
		// TODO Auto-generated method stub
		return CONFIG_DEF;
	}

	@Override
	public void start(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, arg0);
		dbURL = parsedConfig.getString("dbURL");
		userName = parsedConfig.getString("userName");
		userPwd = parsedConfig.getString("userPwd");
		tableName = parsedConfig.getString("tableName");
		primaryKey = parsedConfig.getString("primaryKey");
		topic = parsedConfig.getString("topic");
		gpTable = parsedConfig.getString("gpTable");
		configFile = parsedConfig.getString("configFile");
		primaryTypeStr = parsedConfig.getString("primaryTypeStr");
	}

	@Override
	public void stop() {
	}

	@Override
	public Class<? extends Task> taskClass() {
		return null;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int arg0) {
		ArrayList<Map<String,String>> configs = new ArrayList();
		Map<String, String> config = new HashMap();
		config.put("dbURL", dbURL);
		config.put("userName", userName);
		config.put("userPwd", userPwd);
		config.put("tableName", tableName);
		config.put("primaryKey", primaryKey);
		config.put("topic", topic);
		config.put("gpTable", gpTable);
		config.put("configFile", configFile);
		config.put("primaryTypeStr", primaryTypeStr);
		configs.add(config);
		return configs;
	}

}
