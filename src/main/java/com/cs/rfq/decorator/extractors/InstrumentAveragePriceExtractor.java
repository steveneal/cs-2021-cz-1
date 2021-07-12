package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.utils.FilterDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.*;

public class InstrumentAveragePriceExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        // Filter the Dataset
        // 1. Keep trades for the specific incoming security
        // 2. Keep last one week trades
        Dataset<Row> filtered = trades.filter(trades.col("SecurityId").equalTo(rfq.getIsin()));
        filtered = FilterDataset.filterLastWeek(filtered, "TradeDate");

        double amountAverage = 0;
        double volumeWeightedPrice = 0;

        // Calculate the average price on the filtered
        // Average Price = Sum(Price)/Volume
        if (!filtered.isEmpty()) {
            amountAverage = filtered.agg(avg(col("LastPx"))).first().getDouble(0);
            long volume = filtered.agg(sum(col("LastQty"))).first().getLong(0);
            filtered = filtered.groupBy("LastPx").agg(sum("LastQty").as("volumePerPrice"));
            filtered = filtered.withColumn("volumePerPrice", filtered.col("LastPx").multiply(filtered.col("volumePerPrice")));
            double totalSum = filtered.agg(sum(col("volumePerPrice"))).first().getDouble(0);
            if (volume != 0) {
                volumeWeightedPrice = totalSum / volume;
            }
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(averageTradePricePastWeek, amountAverage);
        results.put(averageTradeVolumeAveragePricePastWeek, volumeWeightedPrice);
        return results;
    }
}
