package com.he;


import java.io.*;
import java.util.Arrays;
import java.util.Base64;

/**
 * Created by Administrator on 2019/7/4.
 */
public class HexToFile {
    public static void main(String[] args) {
        String read = read();
        System.out.println(read);
        write(read);

//        baseDecode();
    }


    public static void baseDecode(){
        Base64.Decoder decoder = Base64.getDecoder();
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\hex.txt"));
//            br = new BufferedReader(new FileReader("D:\\he\\ogg\\blob类型数据"));
            String s;
            while ((s = br.readLine()) != null) {
                sb.append(s);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String read(){
        Base64.Decoder decoder = Base64.getDecoder();
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader("C:\\Users\\Administrator\\Desktop\\hex.txt"));
//            br = new BufferedReader(new FileReader("D:\\he\\ogg\\blob类型数据"));
            String s;
            while ((s = br.readLine())!=null){
                sb.append(s);
            }

            String s1 = new String(decoder.decode(sb.toString()));

            return s1;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void write(String hex){
        if (hex == null || "".equals(hex)){
            return;
        }
        try {
            FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\Administrator\\Desktop\\test.png"));
            byte[] bytes = hex.toUpperCase().getBytes();
            for (int i = 0; i < bytes.length - 1; i+=2) {
                int l = char2Int(bytes[i]) * 16 + char2Int(bytes[i+1]);
                fos.write(l);
            }
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int char2Int(byte ch){
        int val = 0;
        if (ch >= 0x30 && ch <= 0x39){
            val = ch - 0x30;
        } else if (ch >= 0x41 && ch <= 0x46){
            val = ch - 0x41 + 10;
        }
        return val;
    }


}
