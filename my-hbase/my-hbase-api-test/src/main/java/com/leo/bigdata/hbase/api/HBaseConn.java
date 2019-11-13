package com.leo.bigdata.hbase.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
/**
 * 单例链接类
 * @author Administrator
 *
 */
public class HBaseConn {
	private static final HBaseConn INSTANCE = new HBaseConn();
	private static Configuration configuration;
	private static Connection connection;

	private HBaseConn() {
		try {
			if (configuration == null) {
				configuration = HBaseConfiguration.create();
				configuration.set("hbase.zookeeper.quorum", "192.168.182.101");
				configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Connection getConnection() {
		if (connection == null || connection.isClosed()) {
			try {
				connection = ConnectionFactory.createConnection(configuration);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return connection;
	}

	public static Connection getHBaseConn() {
		return INSTANCE.getConnection();
	}

	public static Table getTable(String tableName) throws IOException {
		return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
	}

	public static void closeConn() {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
	}
}
