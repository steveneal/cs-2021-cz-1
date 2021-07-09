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


public class InstrumentLiquidityExtractor implements RfqMetadataExtractor{

    @Override
    public Map<RfqMetadataFieldNames, Object> extractMetaData(Rfq rfq, SparkSession session, Dataset<Row> trades) {

        long VolumePastMonth;
        long todayMs = DateTime.now().withMillisOfDay(0).getMillis();
        long pastMonthMs = DateTime.now().withMillis(todayMs).minusMonths(1).getMillis();

        Dataset<Row> filtered = trades
                .filter(trades.col("SecurityId").equalTo(rfq.getIsin()));

        Dataset<Row> filteredMonth = filtered.filter(trades.col("TradeDate").$greater(new java.sql.Date(pastMonthMs)));


        if (filteredMonth.isEmpty()){
             VolumePastMonth=0;
        }
        else {
             VolumePastMonth = filteredMonth.agg(sum("LastQty")).first().getLong(0);
        }

        Map<RfqMetadataFieldNames, Object> results = new HashMap<>();

        results.put(volumeInstrumentPastMonth, VolumePastMonth);

        return results;
    }

}
