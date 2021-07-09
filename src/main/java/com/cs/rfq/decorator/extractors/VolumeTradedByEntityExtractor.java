package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.sum;

public class VolumeTradedByEntityExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();
        long pastYearMs = DateTime.now().withMillis(todayMs).minusYears(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long volumePastWeek;
        long volumePastMonth;
        long volumePastYear;

        if (filtered.isEmpty()) {
            volumePastWeek = 0;
            volumePastMonth = 0;
            volumePastYear = 0;
        } else {
            volumePastWeek = getVolume(filtered, pastWeekMs);
            volumePastMonth = getVolume(filtered, pastMonthMs);
            volumePastYear = getVolume(filtered, pastYearMs);
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeTradedByEntityPastWeek, volumePastWeek);
        results.put(volumeTradedByEntityPastMonth, volumePastMonth);
        results.put(volumeTradedByEntityPastYear, volumePastYear);
        return results;
    }

    private long getVolume(Dataset<Row> trades, long timeframe) {

        if (trades.filter(trades.col("TradeDate").$greater(new java.sql.Date(timeframe))).isEmpty()) {
            return -1;
        }
        return trades.filter(trades.col("TradeDate").$greater(new java.sql.Date(timeframe))).agg(sum("LastQty")).first().getLong(0);
    }

}
