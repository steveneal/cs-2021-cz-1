package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.*;

public class InstrumentAveragePriceExtractor implements RfqMetadataExtractor {

    private String since;
    public InstrumentAveragePriceExtractor() {
        this.since = DateTime.now().getYear() + "-01-01";
    }

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastWeekMs = DateTime.now().withMillis(todayMs).minusWeeks(1).getMillis();
        double amountAverage;
        double volumeWeightedPrice;

        // Filter the Dataset
        // 1. Keep trades for the specific incoming security
        // 2. Keep last one week trades
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("TradeDate").$greater(new java.sql.Date(pastWeekMs)));

        // Calculate the average price on the filtered
        // Average Price = Sum(Price)/Volume
        if (filtered.isEmpty()) {
             amountAverage = 0;
             volumeWeightedPrice = 0;
        }
        else {
             amountAverage = filtered.agg(avg(col("LastPx"))).first().getDouble(0);
            long volume = filtered.agg(sum(col("LastQty"))).first().getLong(0);
            filtered =  filtered.groupBy("LastPx").agg(sum("LastQty").as("volumePerPrice"));
            filtered = filtered.withColumn("volumePerPrice", filtered.col("LastPx").multiply(filtered.col("volumePerPrice")));
            double totalSum = filtered.agg(sum(col("volumePerPrice"))).first().getDouble(0);
            if (volume != 0){
                volumeWeightedPrice = totalSum/volume;
            }
            else {
                volumeWeightedPrice = 0;
            }
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradePricePastWeek, amountAverage);
        results.put(averageTradeVolumeAveragePricePastWeek, volumeWeightedPrice);
        return results;
    }
}
