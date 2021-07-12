package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.utils.FilterDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.*;
import static org.apache.spark.sql.functions.sum;

public class TradeSideBiasExtractor implements RfqMetadataExtractor {
    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {
        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        Dataset<Row> filteredPastWeek = FilterDataset.filterLastWeek(filtered, "TradeDate");
        Dataset<Row> filteredPastMonth = FilterDataset.filterLastMonth(filtered, "TradeDate");

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(tradeSideBiasPastWeek, this.computeRatio(filteredPastWeek));
        results.put(tradeSideBiasPastMonth, this.computeRatio(filteredPastMonth));

        return results;
    }

    private double computeRatio(Dataset<Row> trades) {
        long sideBuy = 1, sideSell = 2;

        Dataset<Row> filteredBuy = trades.filter(trades.col("Side").equalTo(sideBuy));
        Dataset<Row> filteredSell = trades.filter(trades.col("Side").equalTo(sideSell));

        if (filteredBuy.isEmpty() || filteredSell.isEmpty()) {
            return -1;
        }

        long volumeBuy = filteredBuy.agg(sum("LastQty")).first().getLong(0);
        long volumeSell = filteredSell.agg(sum("LastQty")).first().getLong(0);

        return (double) volumeBuy / volumeSell;
    }
}