package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.*;
import com.cs.rfq.decorator.publishers.MetadataJsonLogPublisher;
import com.cs.rfq.decorator.publishers.MetadataPublisher;
import com.cs.rfq.utils.ConfigReader;
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

        this.trades =  new TradeDataLoader().loadTrades(this.session, ConfigReader.getTradesPath());

        extractors.add(new InstrumentAveragePriceExtractor());
        extractors.add(new InstrumentLiquidityExtractor());
        extractors.add(new TradeSideBiasExtractor());
        extractors.add(new VolumeTradedByEntityExtractor());
        extractors.add(new VolumeTradedForInstrumentByCustomerExtractor());
    }

    public void startSocketListener() throws InterruptedException {
        KafkaConsumer.runConsumer(this);
    }

    public void processRfq(Rfq rfq) {
        log.info(String.format("Received Rfq: %s", rfq.toString()));

        Map<RfqMetadataFieldNames, Object> metadata = new HashMap<>();

        this.extractors.stream()
                .map(extractor -> extractor.extractMetaData(rfq, this.session, this.trades))
                .forEach(metadataMap -> metadataMap.forEach(metadata::put));

        Map<RfqMetadataFieldNames, Object> rfqMap = rfq.toMap();

        rfqMap.forEach(metadata::put);

        this.publisher.publishMetadata(metadata);
    }
}
