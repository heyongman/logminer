package com.he;

import java.sql.Timestamp;

/**
 * Created by Administrator on 2019/7/12.
 */
public class Data {
    private String SCN;
    private String SEG_OWNER;
    private String TABLE_NAME;
    private Timestamp TIMESTAMP;
    private String SQL_REDO;
    private String OPERATION;
    private CurrData data;
    private CurrData before;

    public String getSCN() {
        return SCN;
    }

    public void setSCN(String SCN) {
        this.SCN = SCN;
    }

    public String getSEG_OWNER() {
        return SEG_OWNER;
    }

    public void setSEG_OWNER(String SEG_OWNER) {
        this.SEG_OWNER = SEG_OWNER;
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public Timestamp getTIMESTAMP() {
        return TIMESTAMP;
    }

    public void setTIMESTAMP(Timestamp TIMESTAMP) {
        this.TIMESTAMP = TIMESTAMP;
    }

    public String getSQL_REDO() {
        return SQL_REDO;
    }

    public void setSQL_REDO(String SQL_REDO) {
        this.SQL_REDO = SQL_REDO;
    }

    public String getOPERATION() {
        return OPERATION;
    }

    public void setOPERATION(String OPERATION) {
        this.OPERATION = OPERATION;
    }

    public CurrData getData() {
        return data;
    }

    public void setData(CurrData data) {
        this.data = data;
    }

    public CurrData getBefore() {
        return before;
    }

    public void setBefore(CurrData before) {
        this.before = before;
    }

    @Override
    public String toString() {
        return "Data{" +
                "SCN='" + SCN + '\'' +
                ", SEG_OWNER='" + SEG_OWNER + '\'' +
                ", TABLE_NAME='" + TABLE_NAME + '\'' +
                ", TIMESTAMP=" + TIMESTAMP +
                ", SQL_REDO='" + SQL_REDO + '\'' +
                ", OPERATION='" + OPERATION + '\'' +
                ", data=" + data +
                ", before=" + before +
                '}';
    }
}

class CurrData {
    private String A;
    private String B;
    private String C;

    public String getA() {
        return A;
    }

    public void setA(String a) {
        A = a;
    }

    public String getB() {
        return B;
    }

    public void setB(String b) {
        B = b;
    }

    public String getC() {
        return C;
    }

    public void setC(String c) {
        C = c;
    }

    @Override
    public String toString() {
        return "CurrData{" +
                "A='" + A + '\'' +
                ", B='" + B + '\'' +
                ", C='" + C + '\'' +
                '}';
    }
}
