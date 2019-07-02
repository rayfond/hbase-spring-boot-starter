package ali.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;


public class HBaseDemo {

    private static final String TABLE_NAME = "x1table";
    private static final String CF_DEFAULT = "cf";
    public static final byte[] QUALIFIER = "col1".getBytes();
    private static final byte[] ROWKEY = "rowkey1".getBytes();

    static Connection connection = null;
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        String zkAddress = "hb-proxy-pub-bp177zex2034a00ob-001.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        connection = ConnectionFactory.createConnection(config);
        createTable(TABLE_NAME,config);
        try {

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            try {
                Put put = new Put(ROWKEY);
                put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, "this is value asdfasdfasdfasdf".getBytes());
                table.put(put);
                Get get = new Get(ROWKEY);
                Result r = table.get(get);
                byte[] b = r.getValue(CF_DEFAULT.getBytes(), QUALIFIER);  // returns current version of value
                System.out.println(new String(b));
            } finally {
                if (table != null) table.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static final void createTable(String tableName, Configuration config) throws IOException {

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
        System.out.print("Creating table. ");
        Admin admin = connection.getAdmin();
        admin.createTable(tableDescriptor);
        System.out.println(" Done.");
    }

}
