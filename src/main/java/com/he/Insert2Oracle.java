package com.he;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

/**
 * Created by Administrator on 2019/7/1.
 */
public class Insert2Oracle {

    public static void main(String[] args) {
        FileInputStream fis = null;
        FileReader fileReader = null;
        Connection connect = null;
        PreparedStatement ps = null;
        try {
            connect = new OracleConnection().connect();
            fileReader = new FileReader("C:\\Users\\Administrator\\Desktop\\test.txt");
            fis = new FileInputStream("C:\\Users\\Administrator\\Desktop\\无标题.png");
//            String sql = "INSERT INTO GOLDENGATE.OGG_TEST_3(A,B,C,D,E,F,G,H) VALUES (?,?,?,?,?,?,?,?)";
            String sql = "UPDATE GOLDENGATE.OGG_TEST_3 SET G = ? WHERE B = 'aa'";
            ps = connect.prepareStatement(sql);

//            ps.setBlob(1,fis);
//            ps.setString(2,"aa");
//            ps.setClob(1,fileReader);
//            ps.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
//            ps.setInt(5,346356345);
//            ps.setTimestamp(6,new Timestamp(System.currentTimeMillis()));
            ps.setString(1,"测试字符串111");
//            ps.setDouble(8,99999.8888);

            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (fileReader != null) fileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if (connect != null) {
                    connect.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }
}
