package com.he;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.insert.Insert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.he.OracleConnectorSQL.LOGMINER_SELECT_WITHSCHEMA;
import static com.he.OracleConnectorSchema.*;

/**
 * Created by Administrator on 2019/7/1.
 */
public class LogMinerTest {
    static final Logger log = LoggerFactory.getLogger(LogMinerTest.class);

    public static void main(String[] args) {
        logmnr();
//        String s = parseTableWhiteList(LOGMINER_SELECT_WITHSCHEMA);
//        System.out.println(s);
    }

    public static void logmnr() {
        Connection con = null;
        CallableStatement callableStatement = null;
        CallableStatement logMinerSelect = null;
        ResultSet rs = null;
        FileWriter fw = null;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        try {
            con = new OracleConnection().connect();

            String logMinerStartScr = OracleConnectorSQL.START_LOGMINER_CMD;
            String logMinerOptions = OracleConnectorSQL.LOGMINER_START_OPTIONS;
            logMinerStartScr = logMinerStartScr + logMinerOptions + ") \n; end;";
//            logMinerStartScr=logMinerStartScr+") \n; end;";

//            String logMinerSelectSql = LOGMINER_SELECT_WITHSCHEMA;
            String logMinerSelectSql = parseTableWhiteList(LOGMINER_SELECT_WITHSCHEMA);
            System.out.println("logMinerSelectSql:\n" + logMinerSelectSql);
            System.out.println("logMinerStartScr:\n" + logMinerStartScr);

            callableStatement = con.prepareCall(logMinerStartScr);
//            Long scn = 2188859L;
            Long begin_scn = 6961817L;
            callableStatement.setLong(1, begin_scn);
            callableStatement.execute();

            logMinerSelect = con.prepareCall(logMinerSelectSql);
            logMinerSelect.setFetchSize(1);
            logMinerSelect.setLong(1, begin_scn);
            rs = logMinerSelect.executeQuery();

//            针对lob型处理的全局变量
            Long lastScn = 0L;
            StringBuffer sb = new StringBuffer();

            while (rs.next()) {
                String rowId = rs.getString(ROW_ID_FIELD);
                boolean contSF = rs.getBoolean(CSF_FIELD);
                String segOwner = rs.getString(SEG_OWNER_FIELD);
                String segName = rs.getString(TABLE_NAME_FIELD);
                String sqlRedo = rs.getString(SQL_REDO_FIELD);

//                对超过4000字符的返回数据处理
                while (contSF) {
                    rs.next();
                    sqlRedo += rs.getString(SQL_REDO_FIELD);
                    contSF = rs.getBoolean(CSF_FIELD);
                }


                Timestamp timeStamp = rs.getTimestamp(TIMESTAMP_FIELD);
                String operation = rs.getString(OPERATION_FIELD);
                Long scn = rs.getLong(SCN_FIELD);
                Long commit_scn = rs.getLong(COMMIT_SCN_FIELD);
                Long start_scn = rs.getLong("start_scn");
                Timestamp commit_timestamp = rs.getTimestamp("commit_timestamp");
                String thread = rs.getString("thread#");
                String status = rs.getString("status");
                String sql_undo = rs.getString("sql_undo");
                String info = rs.getString("info");
                String table_name = rs.getString("table_name");
                String username = rs.getString("username");
                String table_space = rs.getString("TABLE_SPACE");
                String session_info = rs.getString("SESSION_INFO");
                String rs_id = rs.getString("RS_ID");
                String ssn = rs.getString("SSN");
                String rbasqn = rs.getString("RBASQN");
                String rbablk = rs.getString("RBABLK");
                String rbabyte = rs.getString("RBABYTE");
                String sequence = rs.getString("SEQUENCE#");
                String tx_name = rs.getString("TX_NAME");
                String seg_type_name = rs.getString("SEG_TYPE_NAME");

//                String log_row = String.format("rowId：%s\noperation：%s\nthread：%s\nstatus：%s\nscn：%s\nbegin_scn：%s\ncommit_scn：%s\nsegOwner：%s\nsegName：%s\ncontSF：%s\ntimeStamp：%s\ncommit_timestamp：%s\nsqlRedo：%s\nsql_undo：%s\ninfo：%s\ntable_name：%s\nusername：%s\ntable_space：%s\nsession_info：%s\nrs_id：%s\nssn：%s\nrbasqn：%s\nrbablk：%s\nrbabyte：%s\nsequence：%s\ntx_name：%s\nseg_type_name：%s\n\n",
//                        rowId, operation, thread, status, scn, start_scn, commit_scn, segOwner, segName, contSF, timeStamp, commit_timestamp, sqlRedo, sql_undo, info, table_name, username, table_space, session_info, rs_id, ssn, rbasqn, rbablk, rbabyte, sequence, tx_name, seg_type_name);
                String log_row = String.format("%s - rowId：%s，\toperation：%s，\tthread：%s，\tstatus：%s，\tscn：%s，\tbegin_scn：%s，\tcommit_scn：%s，\tcommit_timestamp：%s，\trs_id：%s，\tssn：%s，\trbasqn：%s，\trbablk：%s，\trbabyte：%s，\tsequence：%s，\ttx_name：%s，\tseg_type_name：%s，\tsqlRedo：%s\n",
                        sdf.format(new java.util.Date()),rowId, operation, thread, status, scn, start_scn, commit_scn, commit_timestamp, rs_id, ssn, rbasqn, rbablk, rbabyte, sequence, tx_name, seg_type_name,sqlRedo.replaceAll("\r?\n","\t"));

//                 对特殊字段进行处理: lob_write...
                if ((OPERATION_LOB_WRITE.equals(operation))) {
                    if (!lastScn.equals(scn)) {
//                        scn不相等就清空sb
                        sb.setLength(0);
                        log.info("----------------------下一批操作--------------------------");
                    }

//                    解析lob更新语句
                    int sql_begin = sqlRedo.indexOf("BEGIN");
                    sqlRedo = sqlRedo.substring(sql_begin,sqlRedo.length()-1);
                    int sqlEnd = sqlRedo.indexOf(";");

//                     for update语句
                    String sqlUpdate = sqlRedo.substring(7, sqlEnd);

                    sqlRedo = sqlRedo.substring(sqlEnd, sqlRedo.length() - 1);
                    int lobBegin = sqlRedo.indexOf("'")+1;
                    int lobEnd = sqlRedo.lastIndexOf("'");

//                    lob字段内容
                    String lob = sqlRedo.substring(lobBegin, lobEnd);

//                    之后添加到sb中
                    Pattern pattern = Pattern.compile(REGEX_LOB_WRITE, Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(sqlUpdate);
                    if (matcher.find()) {
                        String updateFiled = matcher.group(1);
                        String table = matcher.group(2);
                        String whereCondition = matcher.group(3);
                        sb.append(lob); //追加到sb中
                        sqlRedo = String.format(UPDATE_TEMPLATE, table, updateFiled, sb, whereCondition);

//                        HexToFile.write(sb.toString());

                    }
                } else {
//                    lob_write之外的操作也要清空sb
                    sb.setLength(0);
                }

                log.info("---->RowId：[{}], Operation：[{}], LSCN：[{}], SCN：[{}],  CSCN：[{}], SqlRedo长度：[{}]",rowId,operation,lastScn,scn,commit_scn,sqlRedo.length());
//                String row = String.format("---->RowId：[%s], Operation：[%s], LSCN：[%s], SCN：[%s],  CSCN：[%s], SqlRedo长度：[%s], 最终的sqlRedo：[%s]\n",rowId,operation,lastScn,scn,commit_scn,sqlRedo.length(),sqlRedo);

//                System.out.println(log_row);
                fw = new FileWriter("C:\\Users\\Administrator\\Desktop\\sql.txt", true);
                fw.write(log_row);
                fw.flush();

                net.sf.jsqlparser.statement.Statement parse = CCJSqlParserUtil.parse(sqlRedo);

                String partition = partition(operation,rowId);
                System.out.println(String.format("分区数：%s，分区key：%s",(Math.abs(partition.hashCode()) % 3),partition));

//                System.out.println("parse:"+parse);

//                更新scn
                lastScn = scn;

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String parseTableWhiteList(String logMinerSelectSql) {
        List<String> tabWithSchemas = new ArrayList<>();
        tabWithSchemas.add("HKJGPT.CDC_TEST_2");
        if (tabWithSchemas.isEmpty()) {
            return logMinerSelectSql += "1=1";
        }
        String logMinerSelectWhereStmt = "(";
        for (String tables : tabWithSchemas) {
            List<String> tabs = Arrays.asList(tables.split("\\."));
            logMinerSelectWhereStmt += "(" + SEG_OWNER_FIELD + "='" + tabs.get(0) + "'" + (tabs.get(1).equals("*") ? "" : " and " + TABLE_NAME_FIELD + "='" + tabs.get(1) + "'") + ") or ";
        }
        logMinerSelectWhereStmt = logMinerSelectWhereStmt.substring(0, logMinerSelectWhereStmt.length() - 4) + ")";
        logMinerSelectSql += logMinerSelectWhereStmt;
//        logMinerSelectSql = logMinerSelectSql.replace("((","").replace("))","");
        return logMinerSelectSql;
    }


    private static String partition(String operation,String rowId){
        switch (operation){
            case "INSERT":
                return rowId;

            case "UPDATE":
            case "LOB_WRITE":
            case "DELETE":
                return "a";

            default:
                return "a";

        }
    }


}
