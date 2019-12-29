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
import org.apache.kafka.connect.sink.SinkConnector;

public class GPSinkConnector extends SinkConnector{
	String gpURL;
	String gpUser;
	String gpPwd;
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define("gpURL", Type.STRING, null, Importance.HIGH, "gpURL")
			.define("gpUser", Type.STRING, null, Importance.HIGH, "gpUser")
			.define("gpPwd", Type.STRING, null, Importance.HIGH, "gpPwd");
	
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
		AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, arg0);
		gpURL = parsedConfig.getString("gpURL");
		gpUser = parsedConfig.getString("gpUser");
		gpPwd = parsedConfig.getString("gpPwd");
	}
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Class<? extends Task> taskClass() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public List<Map<String, String>> taskConfigs(int arg0) {
		ArrayList<Map<String,String>> configs = new ArrayList();
		Map<String, String> config = new HashMap();
		config.put("gpURL", gpURL);
		config.put("gpUser", gpUser);
		config.put("gpPwd", gpPwd);
		configs.add(config);
		return configs;
	}
}
