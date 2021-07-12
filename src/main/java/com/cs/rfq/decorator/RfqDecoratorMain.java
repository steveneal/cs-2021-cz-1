package com.cs.rfq.decorator;

import com.cs.rfq.utils.ConfigReader;
import org.apache.spark.sql.SparkSession;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", ConfigReader.getHadoopPath());
        System.setProperty("spark.master", ConfigReader.getLocalCores());

        SparkSession spark = SparkSession
                .builder()
                .master(ConfigReader.getMode())
                .appName(ConfigReader.getAppName())
                .getOrCreate();

        RfqProcessor rfqProcessor = new RfqProcessor(spark);
        rfqProcessor.startSocketListener();
    }
}
