package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class RfqProcessor {

    private final static Logger log = LoggerFactory.getLogger(RfqProcessor.class);

    private final SparkSession session;

    private Dataset<Row> trades;

    private final List<RfqMetadataExtractor> extractors = new ArrayList<>();

    private final MetadataPublisher publisher = new MetadataJsonLogPublisher();

    public RfqProcessor(SparkSession session) {
        this.session = session;

        //TODO: use the TradeDataLoader to load the trade data archives
        TradeDataLoader tradeDataLoader = new TradeDataLoader();
        this.trades = tradeDataLoader.loadTrades(this.session, "src/test/resources/trades/trades.json");

        //TODO: take a close look at how these two extractors are implemented
        extractors.add(new InstrumentAveragePriceExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new TradeSideBiasExtractor());
        extractors.add(new VolumeTradedByEntityExtractor());
        extractors.add(new VolumeTradedForInstrumentByCustomerExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        //TODO: stream data from the input socket on localhost:9000
        KafkaConsumer.runConsumer(this);

        //TODO: start the streaming context
        //this.streamingContext.start();
    }

    public void processRfq(Rfq rfq) {
        System.out.println("here i am");
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        //create a blank map for the metadata to be collected
        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        //TODO: get metadata from each of the extractors
        this.extractors.stream()
                .map(extractor -> {
                    System.out.println("new extractor");
                    return extractor.extractMetaData(rfq, this.session, this.trades);
                })
                .forEach(metadataMap -> metadataMap.forEach(metadata::put));

        System.out.println("here i am now");

        //TODO: publish the metadata
        this.publisher.publishMetadata(metadata);
    }
}
