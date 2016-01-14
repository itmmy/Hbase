package edu.beicai.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/**
 * 
 * HBase示例类，用来讲解HBase旧版JavaAPI的各种用法
 * @author mmy
 * 
 */
public class HBaseDemoForNewAPI {	
	/*
	 * 新版API说明
	 * 新版 API 中加入了 Connection，HAdmin成了Admin，HTable成了Table，而Admin和Table只能通过Connection获得。
	 * Connection的创建是个重量级的操作，由于Connection是线程安全的，所以推荐使用单例，其工厂方法需要一个HBaseConfiguration。
	 * 
	 * 关于Connection接口的说明：
	 * Connection封装了底层与各实际服务器的连接以及与zookeeper的连接。
	 * Connection通过 ConnectionFactory类实例化。Connection的生命周期由调用者维护，调用者通过调用close()，释放资源。
	 * 连接对象包含发现master，定位region，缓存region位置一边当region移动后，重新矫正位置。
	 * 通往各服务器的连接，meta数据缓存，zookeeper连接等，与通过Connection获取的Table，Admin实例都是共享的。
	 * 创建Connection是重量级操作。 Connection是线程安全的，因此，多个客户端线程可以共享一个Connection。
	 * Table和Admin实例，相反地，是轻量级的并且非线程安全。
	 * 典型的用法，一个客户端程序共享一个单独的Connection，每一个线程获取自己的Table实例。
	 * 不建议缓存或者池化（pooling）Table、Admin。
	 * 
	 * 关于ConnectionFactory.createConnection函数的说明：
	 * Connection封装了连接到集群的所有维护工作。
	 * 通过返回的connection生成的table、接口等共享zookeeper连接，meta缓存，到region server以及master的连接。
	 * 调用者负责释放connection实例。 
	 * 
	 */
	public static void main(String[] args) {
		HBaseDemoForNewAPI api = new HBaseDemoForNewAPI();
		api.newAPIDemo();
	}
	
	/**
	 * 创建一个Hbase表
	 * 	使用Hbase1.1.2版本的java API
	 * @param tableName
	 */
	public void newAPIDemo(){
		
		/*需要准备的数据*/
		String tableName = "testTable";
		String rowKey = "rk1";
		String columFamilyName = "cf1";
		String qualifier = "q1";
		String value ="ssssss";
		
		// ***新版API*** 创建HBase配置对象（HBase配置对象继承自Hadoop的Configuration，这里使用父类的引用指向子类的对象的设计）
		Configuration conf = HBaseConfiguration.create();
		
		// ***新版API*** 使用连接工厂根据配置器创建与HBase之间的连接对象
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// ***新版API*** 实例化 tableDescriptor类
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		// 通过表描述对象往表中添加列族
		tableDescriptor.addFamily(new HColumnDescriptor(columFamilyName));
		
		try {
			
			// ***新版API*** 初始化Hbase的管理员
			Admin admin = connection.getAdmin();
			
			// 使用Hbase管理员创建表
			admin.createTable(tableDescriptor);
			System.out.println(" Table "+tableName+" 已经被创建 ");
			
			// ***新版API*** 创建旧的“HTable”对象，在新的API中HTable已经改成Table了
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			//使用put对象封装需要添加的信息
			Put put = new Put(rowKey.getBytes());
			
			// ***新版API*** 往Put对象上添加信息 旧版API中的于此相同的方法为add，此处改版后为addColumn
			put.addColumn(columFamilyName.getBytes(), qualifier.getBytes(),value.getBytes());
			
			//真正将put中的内容添加到表中
			table.put(put);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
