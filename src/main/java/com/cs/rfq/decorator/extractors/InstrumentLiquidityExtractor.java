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


public class InstrumentLiquidityExtractor implements RfqMetadataExtractor{

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {


        Dataset<Row> filtered = trades.filter(trades.col("SecurityId").equalTo(rfq.getIsin()));
        Dataset<Row> filteredMonth = FilterDataset.filterLastMonth(filtered, "TradeDate");

        long volumePastMonth = 0;

        if (!filteredMonth.isEmpty()){
            volumePastMonth = filteredMonth.agg(sum("LastQty")).first().getLong(0);
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(liquidity, volumePastMonth);
        return results;
    }

}
