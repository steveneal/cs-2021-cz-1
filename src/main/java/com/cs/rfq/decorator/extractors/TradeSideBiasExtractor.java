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

public class TradeSideBiasExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();


        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Dataset<Row> filteredPastWeek = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)));
        Dataset<Row> filteredPastMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)));

        double tradeRatioPastWeek = this.computeRatio(filteredPastWeek);
        double tradeRatioPastMonth = this.computeRatio(filteredPastMonth);

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeSideBiasPastWeek, tradeRatioPastWeek);
        results.put(tradeSideBiasPastMonth, tradeRatioPastMonth);

        return results;
    }

    private double computeRatio(Dataset<Row> trades) {
        long sideBuy = 1, sideSell = 2;
        try
        long volumeBuy = trades.filter(trades.col("Side").equalTo(sideBuy)).agg(sum("LastQty")).first().getLong(0);
        long volumeSell = trades.filter(trades.col("Side").equalTo(sideSell)).agg(sum("LastQty")).first().getLong(0);

        if (volumeBuy==0 || volumeSell==0){
            return -1;
        }

        return (double) volumeBuy / volumeSell;
    }
}