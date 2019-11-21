package com.he;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2019/7/4.
 * 配置管理类
 */
public class Configuration {
    private Properties prop = null;

    /**
     *
     * @param path resources的相对路径
     * @return
     */
    public static Configuration from (String path){
        InputStream is = Configuration.class.getClassLoader().getResourceAsStream(path);
        Properties prop = new Properties();
        try {
            prop.load(is);
        }catch (Exception e){
            e.printStackTrace();
        }
        return new Configuration(prop);
    }

    private Configuration(Properties prop) {
        this.prop = prop;
    }

    public String getString(String key){
        return this.prop.getProperty(key);
    }

    public Integer getInteger(String key){
        try {
            return Integer.valueOf(getString(key));
        } catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public Boolean getBoolean(String key){
        try {
            return Boolean.valueOf(getString(key));
        } catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public Long getLong(String key){
        try {
            return Long.valueOf(getString(key));
        } catch (Exception e){
            e.printStackTrace();
        }
        return 0L;
    }
}
