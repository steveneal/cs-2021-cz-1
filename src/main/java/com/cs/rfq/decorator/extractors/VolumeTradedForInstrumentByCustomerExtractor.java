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
import static org.apache.spark.sql.functions.sum;

public class VolumeTradedForInstrumentByCustomerExtractor implements RfqMetadataExtractor {

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()))
                .filter(trades.col("EntityId").equalTo(rfq.getEntityId()));

        long volumePastWeek = 0;
        long volumePastMonth = 0;
        long volumePastYear = 0;

        if (!filtered.isEmpty()) {
            volumePastWeek = getVolume(FilterDataset.filterLastWeek(filtered, "TradeDate"));
            volumePastMonth = getVolume(FilterDataset.filterLastMonth(filtered, "TradeDate"));
            volumePastYear = getVolume(FilterDataset.filterLastYear(filtered, "TradeDate"));
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();
        results.put(volumeInstrumentPastWeek, volumePastWeek);
        results.put(volumeInstrumentPastMonth, volumePastMonth);
        results.put(volumeInstrumentPastYear, volumePastYear);
        return results;
    }

    private long getVolume(Dataset<Row> trades) {

        if (trades.isEmpty()) {
            return 0;
        }
        return trades.agg(sum("LastQty")).first().getLong(0);
    }

}
