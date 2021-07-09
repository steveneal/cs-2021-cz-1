package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VolumeTradedByEntityExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(4461279226039677777L);
    }

    @Test
    public void checkVolumeTradedByEntityPastWeek() {

        String filePath = getClass().getResource("volume-traded-by-entity.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByEntityExtractor extractor = new VolumeTradedByEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastWeek);

        assertEquals(180L, result);
    }

    @Test
    public void checkVolumeTradedByEntityPastMonth() {

        String filePath = getClass().getResource("volume-traded-by-entity.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByEntityExtractor extractor = new VolumeTradedByEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastMonth);

        assertEquals(240L, result);
    }

    @Test
    public void checkVolumeTradedByEntityPastYear() {

        String filePath = getClass().getResource("volume-traded-by-entity.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByEntityExtractor extractor = new VolumeTradedByEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastYear);

        assertEquals(300L, result);
    }

    @Test
    public void checkVolumeTradedByEntityWhenNoEntityMatch() {

        Rfq rfq1 = new Rfq();
        rfq1.setEntityId(12345L);

        String filePath = getClass().getResource("volume-traded-by-entity.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByEntityExtractor extractor = new VolumeTradedByEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq1, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastWeek);
        Object result1 = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastMonth);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastYear);


        assertEquals(0L, result);
        assertEquals(0L, result1);
        assertEquals(0L, result2);
    }

    @Test
    public void checkVolumeTradedByEntityIfThereIsNoTradesForTheTimeFrame() {
        Rfq rfq2 = new Rfq();
        rfq2.setEntityId(5561279226039699999L);

        String filePath = getClass().getResource("volume-traded-by-entity.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedByEntityExtractor extractor = new VolumeTradedByEntityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq2, session, trades);

        Object resultWeek = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastWeek);
        Object resultMonth = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastMonth);
        Object resultYear = meta.get(RfqMetadataFieldNames.volumeTradedByEntityPastYear);

        assertEquals(0L, resultWeek);
        assertEquals(0L, resultMonth);
        assertEquals(100L, resultYear);
    }
}
