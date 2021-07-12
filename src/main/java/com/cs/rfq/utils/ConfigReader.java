package com.cs.rfq.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {

    private ConfigReader() {
    }

    private static Properties prop;

    static {
        prop = new Properties();
        String fileName = "src/main/resources/configs/app.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            prop.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getHadoopPath(){
        return prop.getProperty("hadoopPath");
    }

    public static String getMode(){
        return prop.getProperty("mode");
    }

    public static String getLocalCores(){
        return prop.getProperty("localCores");
    }

    public static String getAppName(){
        return prop.getProperty("appName");
    }

    public static String getTradesPath(){
        return prop.getProperty("tradesPath");
    }

}
