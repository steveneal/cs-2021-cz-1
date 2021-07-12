package com.cs.rfq.decorator;

import org.apache.spark.sql.SparkSession;

public class RfqDecoratorMain {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Java\\hadoop-2.9.2");
        System.setProperty("spark.master", "local[4]");

        //TODO: create a Spark session
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("JavaALSExample")
                .getOrCreate();

        //TODO: create a new RfqProcessor and set it listening for incoming RFQs
        RfqProcessor rfqProcessor = new RfqProcessor(spark);
        rfqProcessor.startSocketListener();
    }

}
