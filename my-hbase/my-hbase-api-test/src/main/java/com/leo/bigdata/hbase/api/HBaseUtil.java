package com.leo.bigdata.hbase.api;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ������
 * 
 * @author Administrator
 *
 */
public class HBaseUtil {
	/**
	 * ����HBase��
	 * 
	 * @param tableName ����
	 * @param cfs       ����Ĕ��M
	 * @return �Ƿ񄓽��ɹ�
	 */
	public static boolean createTable(String tableName, String[] cfs) {
		try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
			if (admin.tableExists(tableName)) {
				return false;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			Arrays.stream(cfs).forEach(cf -> {
				HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
				columnDescriptor.setMaxVersions(1);
				tableDescriptor.addFamily(columnDescriptor);
			});
			admin.createTable(tableDescriptor);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return true;
	}

	/**
	 * ɾ��hbase��.
	 *
	 * @param tableName ����
	 * @return �Ƿ�ɾ���ɹ�
	 */
	public static boolean deleteTable(String tableName) {
		try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * HBase����һ�l����
	 * 
	 * @param tableName ����
	 * @param rowKey    Ψһ���R
	 * @param cfName    ������
	 * @param qualifier �И��R
	 * @param data      ����
	 * @return �Ƿ����ɹ�
	 */
	public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
			table.put(put);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return true;
	}

	public static boolean putRows(String tableName, List<Put> puts) {
		try (Table table = HBaseConn.getTable(tableName)) {
			table.put(puts);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return true;
	}

	/**
	 * ��ȡ��������
	 * 
	 * @param tableName ����
	 * @param rowKey    Ψһ��ʶ
	 * @return ��ѯ���
	 */
	public static Result getRow(String tableName, String rowKey) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Get get = new Get(Bytes.toBytes(rowKey));
			return table.get(get);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public static Result getRow(String tableName, String rowKey, FilterList filterList) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.setFilter(filterList);
			return table.get(get);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public static ResultScanner getScanner(String tableName) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Scan scan = new Scan();
			scan.setCaching(1000);
			return table.getScanner(scan);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	/**
	 * ������������
	 * 
	 * @param tableName   ����
	 * @param startRowKey ��ʼRowKey
	 * @param endRowKey   ��ֹRowKey
	 * @return {@link ResultScanner}ʵ��
	 */
	public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			scan.setCaching(1000);
			return table.getScanner(scan);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
			FilterList filterList) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRowKey));
			scan.setStopRow(Bytes.toBytes(endRowKey));
			scan.setFilter(filterList);
			scan.setCaching(1000);
			return table.getScanner(scan);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return null;
	}

	/**
	 * HBase ɾ��һ�м�¼
	 * 
	 * @param tableName ����
	 * @param rowKey    Ψһ��ʶ
	 * @return �Ƿ�ɾ���ɹ�
	 */
	public static boolean deleteRow(String tableName, String rowKey) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			table.delete(delete);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return true;
	}

	public static boolean deleteColumnFamily(String tableName, String cfName) {
		try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
			admin.deleteColumn(tableName, cfName);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	public static boolean deleteQualifier(String tableName, String rowKey, String cfName, String qualifier) {
		try (Table table = HBaseConn.getTable(tableName)) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier));
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}
