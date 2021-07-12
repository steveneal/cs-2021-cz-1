package com.cs.rfq.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ConfigReader {

    private ConfigReader() {
    }

    public static void getProperties() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/configs/app.config";
        try (FileInputStream fis = new FileInputStream(fileName)) {
            prop.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(prop.getProperty("app.name"));
    }

}
