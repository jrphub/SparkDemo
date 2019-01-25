package com.spark.practice.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class NativeHBaseAPI {

	public static final String TABLE = "PRWATECH_TEST"; // Table names are
													// case-sensitive

	public static void main(String[] args) throws IOException {
		//to run as different user which is a hadoop user
		System.setProperty("HADOOP_USER_NAME", "huser");
		//Hadoop and HBASE needs to be started before running this program
		Configuration hconfig = HBaseConfiguration.create();

		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(TABLE));
		htable.addFamily(new HColumnDescriptor("personal")); //Add column family
		htable.addFamily(new HColumnDescriptor("professional"));

		System.out.println("Connecting...");
		Connection conn = ConnectionFactory.createConnection(hconfig);
		Admin hAdmin = conn.getAdmin();
		
		System.out.println("Creating Table...");
		
		//create 'PRWATECH_TEST', 'personal', 'professional'
		hAdmin.createTable(htable);
		
		//Getting all the list of tables using HBaseAdmin object
		System.out.println("Listing Tables and corresponding Column Families");
		HTableDescriptor[] tableDescriptor =hAdmin.listTables();
		
		for (HTableDescriptor tblDescriptor : tableDescriptor) {
			System.out.println(tblDescriptor.getNameAsString());
			System.out.println(tblDescriptor.getColumnFamilies().length);
			//Listing column families of a table
			for (HColumnDescriptor colDescriptor : tblDescriptor.getColumnFamilies()) {
				System.out.println(colDescriptor.getNameAsString());
			}
		}
		
		//Insert
		System.out.println("Populating Table");
		//HTable hTable = new HTable(hconfig, TABLE);
		Table hTable = conn.getTable(TableName.valueOf(TABLE));


		Put p = new Put(Bytes.toBytes("101"));
		//put 'PRWATECH_TEST', '101', 'personal:name', 'jyoti'
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"),Bytes.toBytes("jyoti"));
		//put 'PRWATECH_TEST', '101', 'personal:city', 'pune'
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"),Bytes.toBytes("pune"));
		
		//put 'PRWATECH_TEST', '101', 'professional:dept', 'engineering'
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("dept"),Bytes.toBytes("engineering"));
		
		hTable.put(p);
		
		
		//Read
		//get 'PRWATECH_TEST', '101'
		//or get 'PRWATECH_TEST', ‘101’, {COLUMN => ‘personal:city’}
		System.out.println("Reading city column of record 101");
		Get get = new Get(Bytes.toBytes("101"));
		//get.addFamily(Bytes.toBytes("personal"));
		get.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		
		Result result = hTable.get(get);
		byte [] value = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
		String city = Bytes.toString(value);
		System.out.println("for 101 : city : " + city);

		
		
		//Update
		System.out.println("Updating record 101 : city -> Bangalore");
		//put ‘PRWATECH_TEST’,’101’,'personal:city',’Bangalore’
		Put pt = new Put(Bytes.toBytes("101"));
		pt.add(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Bangalore"));
		hTable.put(pt);
		
		//Delete
		System.out.println("Deleting record 101 ...");
		//delete ‘PRWATECH_TEST’, ‘101’, ‘personal:city’, ‘<time stamp>’ 
		//deleteall ’PRWATECH_TEST’,’101’
		Delete delete = new Delete(Bytes.toBytes("101"));
		//delete.deleteColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		hTable.delete(delete);
		
		hTable.close();
		
		System.out.println("Done!");
		hAdmin.close();
	}

}