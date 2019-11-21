package com.he;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.he.OracleConnectorSchema.REGEX_LOB_WRITE;
import static com.he.OracleConnectorSchema.UPDATE_TEMPLATE;

/**
 * Created by Administrator on 2019/7/10.
 */
public class MyTest {
    static final Logger log = LoggerFactory.getLogger(MyTest.class);

    @Test
    public void test1() {
        String sql = "update \"HKJGPT\".\"CDC_TEST\" set \"C\" = 'ww' where \"A\" = 'f' and \"B\" = 3";
        FileReader fileReader = null;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("update \"HKJGPT\".\"CDC_TEST\" set \"C\" = '");
            fileReader = new FileReader("C:\\Users\\Administrator\\Desktop\\无标题.png");
            int ch = 0;
            char[] b = new char[1024];
            while ((ch = fileReader.read(b)) != -1) {
                sb.append(b);
            }
            sb.append("' where \"A\" = 'f' and \"B\" = 3");

            System.out.println(sb.length());
            Statement parse = CCJSqlParserUtil.parse(sb.toString());
            if (parse instanceof Update) {
                Update update = (Update) parse;
                List<Column> columns = update.getColumns();
                List<Expression> expressions = update.getExpressions();

                System.out.println(columns);
                System.out.println(expressions.get(0).toString().length());
                HexToFile.write(expressions.get(0).toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void test2() {
        String json = "{\"SCN\":6132582,\"SEG_OWNER\":\"HKJGPT\",\"TABLE_NAME\":\"CDC_TEST\",\"TIMESTAMP\":1562757509000,\"SQL_REDO\":\"update \\\"HKJGPT\\\".\\\"CDC_TEST\\\" set \\\"B\\\" = 4 where \\\"A\\\" = 'e' and \\\"B\\\" = 3\",\"OPERATION\":\"UPDATE\",\"data\":{\"A\":\"e\",\"B\":4.0,\"C\":null},\"before\":{\"A\":\"e\",\"B\":3.0,\"C\":null}}";
        Data data = JSON.parseObject(json, Data.class);

        System.out.println(data);

        Data data1 = new Data();
        System.out.println(data1.getSCN());

    }


    @Test
    public void test3() {
        StringBuffer sb = new StringBuffer();
        sb.append("eraer");
        sb.append("45er5e");
        System.out.println(sb);
        sb.setLength(0);
        System.out.println(sb);
        sb.append("w3w");
        System.out.println(sb);
    }

    @Test
    public void test4() {
        FileOutputStream fos = null;
        Connection connect = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            fos = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\test.png");
            connect = new OracleConnection().connect();

            String sql = "SELECT A,J FROM CDC_TEST_3 WHERE rownum = 1";
            ps = connect.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String string = rs.getString(1);
                Blob blob = rs.getBlob(2);
                InputStream binaryStream = blob.getBinaryStream();
                System.out.println(string);

                byte[] b = new byte[1024];
                int w = 0;
                while ((w = binaryStream.read(b)) != -1) {
                    fos.write(b, 0, w);
                    w += w;
                }

            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                assert connect != null;
                connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                assert ps != null;
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                assert rs != null;
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void test5(){
        {
            Properties props = new Properties();
            Configuration conf = Configuration.from("kafka.properties");

            props.put("bootstrap.servers", conf.getString("bootstrap.servers"));
            props.put("group.id", "connect_1");
            props.put("enable.auto.commit", conf.getString("enable.auto.commit"));
            props.put("key.deserializer", conf.getString("key.deserializer"));
            props.put("value.deserializer", conf.getString("value.deserializer"));
            props.put("auto.offset.reset", "earliest");

            ArrayList<String> topics = new ArrayList<>();
            topics.add("cdctest");


            KafkaConsumer<String, String> consume = new KafkaConsumer<>(props);
            consume.subscribe(topics);

            while (true) {
                //1000ms拉取一次
                ConsumerRecords<String, String> records = consume.poll(1000);
                for (ConsumerRecord record : records) {
                    String kafkaKey = record.key() == null ? "invalidKey" : record.key().toString();
                    String kafkaValue = record.value().toString();

                    System.out.printf("key:%s\t\tvalue:%s\n",kafkaKey,kafkaValue);
//                    System.out.printf("key:%s",kafkaKey);


                }
                //手動同步offset
//                consume.commitSync();
            }
        }
    }


    @Test
    public void test6(){
        String s = "DECLARE \n" +
                " loc_c CLOB; \n" +
                " buf_c VARCHAR2(6156); \n" +
                " loc_b BLOB; \n" +
                " buf_b RAW(6156); \n" +
                " loc_nc NCLOB; \n" +
                " buf_nc NVARCHAR2(6156); \n" +
                " e_len NUMBER; \n" +
                "BEGIN\n" +
                " select \"B\" into loc_c from \"HKJGPT\".\"CDC_TEST1\" where \"A\" = 2 for update;\n" +
                "\n" +
                " buf_c := '\n" +
                "\n" +
                "\n" +
                "�Ѹ���: 23:29:07\n" +
                "Zabbix 3.4.15. ? 2001�C2018, Zabbix SIA'; \n" +
                "  dbms_lob.write(loc_c, 62, 3073, buf_c);\n" +
                "END;";

        int begin = s.indexOf("BEGIN");
        s = s.substring(begin,s.length()-1);
        int end = s.indexOf(";");
        System.out.println(begin+","+end);
        String s1 = s.substring(7, end); //for update语句
        System.out.println("["+s1+"]");


//        for update以及之后的部分
        s = s.substring(end, s.length() - 1);
        int lob_begin = s.indexOf("'");
        int lob_end = s.lastIndexOf("'");
        int buf = s.indexOf("buf");
        String lob_type = s.substring(buf, buf + 1);
        System.out.println(s);

//        lob内容
        String s2 = s.substring(lob_begin+1, lob_end);

        System.out.println(s2);

        Pattern pattern = Pattern.compile(REGEX_LOB_WRITE, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(s1);
        if (matcher.find()) {
            String updateFiled = matcher.group(1);
            String table = matcher.group(2);
            String whereCondition = matcher.group(3);
            System.out.println(String.format("%s, %s, %s",updateFiled,table,whereCondition));
        }

    }

    @Test
    public void test7(){
        String o = "{\"payload\":\"AAAAAAAAAAAAAAAAAA\"}";
        JSONObject jsonObject = JSON.parseObject(o);
        int hash = Utils.abs(jsonObject.hashCode());
        System.out.println(hash+", "+hash % 3);
    }

    @Test
    public void test8(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format = sdf.format(new Date());
        System.out.println(format);

    }


    @Test
    public void test9(){
        String host = "104.1.12.234";
        String port = "1521";
        String dbName = "ORCL";
        String user = "hksix";
        String password = "hiksix";
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection connection = null;
        try {
             connection = DriverManager.getConnection("jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName, user, password);
//            String sql = "insert into HKSIX.TEST1(A,B) VALUES (?,?)";
//            ps = connection.prepareStatement(sql);
//            ps.setString(1,null);
//            ps.setString(2,null);
//
//            ps.execute();


            String sql = "select dbms_metadata.get_ddl('INDEX','IDX_DRV_FLOW_GLBM_XYGW','HKSIX') from dual";
//            String sql = "select * from user_indexes where table_name = 'DRV_FLOW'";
//            String sql = "select * from user_ind_columns where index_name = 'IDX_DRV_FLOW_GLBM_XYGW'";
            ps = connection.prepareCall(sql);
            rs = ps.executeQuery();
            while (rs.next()){
                String string = rs.getString(1);
//                String string1 = rs.getString(2);
                System.out.println(string);
//                System.out.println(string1);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void test10(){
        String host = "104.5.1.5";
        String port = "1521";
        String dbName = "racdg";
        String user = "hkjgpt";
        String password = "hkjgpt";
        java.sql.Statement s = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName, user, password);
//            String sql = "select dbms_metadata.get_granted_ddl('OBJECT_GRANT','HKJGPT') from dual";
//            String sql = "select dbms_metadata.get_ddl('USER','TRFF_APP') from dual";
            String sql = "select dbms_metadata.get_ddl('TABLESPACE','TS.tablespace_name') from DBA_TABLESPACES TS";
//            ps = connection.prepareCall(sql);
            s = connection.createStatement();
            rs = s.executeQuery(sql);
            while (rs.next()){
                String string = rs.getString(1);
//                String string1 = rs.getString(2);
                System.out.println(string);
//                System.out.println(string1);
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (s != null) {
                    s.close();
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
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void test11(){
        java.sql.Date date = new java.sql.Date(new Date().getTime());
        System.out.println(date);

        System.out.println(new Date());
    }

    @Test
    public void test12() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
        Date parse = sdf.parse("2019-07-28 18:43:06.729323432");
        LocalDateTime t = LocalDateTime.parse("2019-07-28 18:43:06.729323432".replace(" ", "T"));

        System.out.println(Timestamp.valueOf(t));

        Timestamp timestamp = Timestamp.valueOf("0000-00-00 00:00:00");
        System.out.println(timestamp);
    }

}
