package com.cs.rfq.decorator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.types.DataTypes.*;

public class TradeDataLoader {

    private final static Logger log = LoggerFactory.getLogger(TradeDataLoader.class);

    public Dataset<Row> loadTrades(SparkSession session, String path) {
        StructType schema = new StructType(new StructField[]{
                new StructField("TraderId", LongType, false, Metadata.empty()),
                new StructField("EntityId", LongType, false, Metadata.empty()),
                new StructField("SecurityID", StringType, false, Metadata.empty()),
                new StructField("LastQty", LongType, false, Metadata.empty()),
                new StructField("LastPx", DoubleType, false, Metadata.empty()),
                new StructField("TradeDate", DateType, false, Metadata.empty()),
                new StructField("Currency", StringType, false, Metadata.empty()),
                new StructField("Side", IntegerType, false, Metadata.empty()),
                new StructField("OrderID", LongType, false, Metadata.empty())
        });

        Dataset<Row> trades = session.read().schema(schema).json(path);

        log.info(trades.count() + " trades have been loaded using the following schema: " + schema);
        return trades;
    }

}
