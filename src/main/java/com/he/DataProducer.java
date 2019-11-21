package com.he;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;

/**
 * Created by Administrator on 2019/7/6.
 */
public class DataProducer {
    public static SimpleDateFormat sdf;
    public static void main(String[] args) {
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Thread t1 = new Thread(DataProducer::write2Test1);
        Thread t2 = new Thread(DataProducer::write2Test2);
        Thread t3 = new Thread(DataProducer::write2Test3);

        t1.start();
        t2.start();
        t3.start();
    }

    public static void write2Test1(){
        Connection connect = null;
        PreparedStatement ps = null;

        try {
            connect = new OracleConnection().connect();
            connect.setAutoCommit(false);

            String sql = "INSERT INTO HKJGPT.CDC_TEST_1 (A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ps = connect.prepareStatement(sql);
            Long s = null;
//            30000000
            for (int i = 0; i < 150000000; i++) {
                ps.setString(1,"测试字符串-"+i);
                ps.setString(2,"测试字符串-"+i);
                ps.setInt(3,1);
                ps.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
                ps.setDate(5, java.sql.Date.valueOf("2019-07-06"));
                ps.setString(6,"测试字符串-"+i);
                ps.setString(7,"测试字符串-"+i);
                ps.setString(8,"测试字符串-"+i);
                ps.setString(9,"测试字符串-"+i);
                ps.setString(10,"测试字符串-"+i);
                ps.setFloat(11,232323.66f);
                ps.setDate(12,java.sql.Date.valueOf("2019-07-06"));
                ps.setFloat(13,232323.66f);
                ps.setFloat(14,232323.66f);
                ps.setString(15,"测试字符串-"+i);
                ps.setInt(16,3442343);

                ps.addBatch();

                if (i % 1000 == 0) {
                    ps.executeBatch();
                    connect.commit();
                    ps.clearBatch();
                    Thread.sleep(500);
                }

                if (i % 2000 == 0){
                    if (s == null){
                        s = System.currentTimeMillis();
                    } else {
                        long e = System.currentTimeMillis();
                        System.out.printf("%s Test1 现已插入：%s 条， 平均每秒插入数据：%.0f 条，  每天插入：%.2f 万条\n",sdf.format(e),i,(2000*1000.0/(e-s)),(2000*1000.0*3600*24/((e-s)*10000))) ;
                        s = System.currentTimeMillis();
                    }
                }
            }

            ps.executeBatch();
            connect.commit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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

    public static void write2Test2(){
        Connection connect = null;
        PreparedStatement ps = null;
        try {
            connect = new OracleConnection().connect();
            connect.setAutoCommit(false);

            String sql = "INSERT INTO HKJGPT.CDC_TEST_2 (A,B,C,D) VALUES (?,?,?,?)";
            ps = connect.prepareStatement(sql);
            Long s = null;
            for (int i = 0; i < 150000000; i++) {
                ps.setString(1,"测试字符串-"+i);
                ps.setString(2,"测试字符串-"+i);
                ps.setTimestamp(3,new Timestamp(System.currentTimeMillis()));
                ps.setString(4,"测试字符串-"+i);

                ps.addBatch();

                if (i % 1000 == 0) {
                    ps.executeBatch();
                    connect.commit();
                    ps.clearBatch();
                    Thread.sleep(500);
                }
                if (i % 2000 == 0){
                    if (s == null){
                        s = System.currentTimeMillis();
                    } else {
                        long e = System.currentTimeMillis();
                        System.out.printf("%s Test2 现已插入：%s 条， 平均每秒插入数据：%.0f 条，  每天插入：%.2f 万条\n",sdf.format(e),i,(2000*1000.0/(e-s)),(2000*1000.0*3600*24/((e-s)*10000))) ;
                        s = System.currentTimeMillis();
                    }
                }
            }

            ps.executeBatch();
            connect.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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

    public static void write2Test3(){
        FileInputStream fis = null;
        Connection connect = null;
        PreparedStatement ps = null;
        try {
            connect = new OracleConnection().connect();
            connect.setAutoCommit(false);

            String sql = "INSERT INTO HKJGPT.CDC_TEST_3 (A,B,C,D,E,F,G,H,I,J) VALUES (?,?,?,?,?,?,?,?,?,?)";
            ps = connect.prepareStatement(sql);
            Long s = null;

            for (int i = 0; i < 500000; i++) {
                fis = new FileInputStream("/tmp/he/test.png") ;
//                fis = new FileInputStream("C:\\Users\\Administrator\\Desktop\\无标题.png") ;

                ps.setString(1,"测试字符串-"+i);
                ps.setString(2,"测试字符串-"+i);
                ps.setString(3,"测试字符串-"+i);
                ps.setString(4,"测试字符串-"+i);
                ps.setInt(5,i);
                ps.setString(6,"测试字符串-"+i);
                ps.setTimestamp(7,new Timestamp(System.currentTimeMillis()));
                ps.setInt(8,i);
                ps.setString(9,"测试字符串-"+i);
                ps.setBlob(10,fis);

                ps.addBatch();
                if (i % 10 == 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                    connect.commit();
                    Thread.sleep(1000);
                }
                if (i % 100 == 0){
                    if (s == null){
                        s = System.currentTimeMillis();
                    } else {
                        long e = System.currentTimeMillis();
                        System.out.printf("%s Test3 现已插入：%s 条， 平均每秒插入数据：%.0f 条，   数据大小：%.2f KB，  每天插入 %.2f GB\n",sdf.format(e),i,(100*1000.0/(e-s)),(100*1000.0*32.2/(e-s)),(100*1000.0*32.2*3600*24/((e-s)*1024*1024))) ;
                        s = System.currentTimeMillis();
                    }
                }
            }

            ps.executeBatch();
            connect.commit();
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
