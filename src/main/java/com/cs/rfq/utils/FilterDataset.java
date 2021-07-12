package com.cs.rfq.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;

public class FilterDataset {

    private FilterDataset() {}

    public static Dataset<Row> filterLastWeek(Dataset<Row> dataset, String col){
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        return dataset.filter(dataset.col(col).$greater(new java.sql.Date(pastWeekMs)));
    }

    public static Dataset<Row> filterLastMonth(Dataset<Row> dataset, String col){
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        return dataset.filter(dataset.col(col).$greater(new java.sql.Date(pastMonthMs)));
    }

    public static Dataset<Row> filterLastYear(Dataset<Row> dataset, String col){
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();
        return dataset.filter(dataset.col(col).$greater(new java.sql.Date(pastYearMs)));
    }

}
