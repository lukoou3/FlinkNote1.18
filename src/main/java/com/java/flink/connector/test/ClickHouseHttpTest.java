package com.java.flink.connector.test;

import com.clickhouse.client.data.ClickHouseBitmap;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class ClickHouseHttpTest {

    @Test
    public void test() throws Exception{
        String url = "jdbc:clickhouse://192.168.216.86:8123";
        Properties props = new Properties();
        //props.put("continueBatchOnError", "true");
        props.put("user", "default");
        props.put("password", "123456");

        Connection conn = DriverManager.getConnection(url, props);

        String sql = "insert into test.test_bitmap select datetime, name, value from input('datetime UInt32, name String, value AggregateFunction(groupBitmap, UInt32)')";
        try {
            PreparedStatement stmt = conn.prepareStatement(sql);
            Object[][] objs = new Object[][] {
                    new Object[] { 1614566500, "a", ClickHouseBitmap.wrap(1, 2, 3, 4, 5) },
                    new Object[] { 1614566500, "b",  ClickHouseBitmap.wrap(6, 7, 8, 9, 10) },
                    new Object[] { 1614566500, "c", ClickHouseBitmap.wrap(11, 12, 13) }
            };

            for (Object[] v : objs) {
                int i = 1;
                stmt.setInt(i++, (int) v[0]);
                stmt.setString(i++, (String) v[1]);
                stmt.setObject(i++, v[2]);
                stmt.addBatch();
            }
            int[] results = stmt.executeBatch();
            System.out.println(results);

            stmt.close();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            conn.close();
        }

    }

}
