package com.leo.bigdata.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PhoenixTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection connection = DriverManager.getConnection("jdbc:phoenix:localhost:2181");

		PreparedStatement statement = connection.prepareStatement("select * from PERSON");

		ResultSet resultSet = statement.executeQuery();

		while (resultSet.next()) {
			System.out.println(resultSet.getString("NAME"));
		}

		statement.close();
		connection.close();
	}

}
