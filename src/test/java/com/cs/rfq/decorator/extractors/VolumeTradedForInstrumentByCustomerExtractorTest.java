package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class VolumeTradedForInstrumentByCustomerExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkVolumeInstrumentPastWeek() {

        String filePath = getClass().getResource("volume-instrument-test.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentByCustomerExtractor extractor = new VolumeTradedForInstrumentByCustomerExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastWeek);

        assertEquals(400000L, result);
    }

    @Test
    public void checkVolumeInstrumentPastMonth() {

        String filePath = getClass().getResource("volume-instrument-test.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentByCustomerExtractor extractor = new VolumeTradedForInstrumentByCustomerExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastMonth);

        assertEquals(750000L, result);
    }

    @Test
    public void checkVolumeInstrumentPastYear() {

        String filePath = getClass().getResource("volume-instrument-test.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentByCustomerExtractor extractor = new VolumeTradedForInstrumentByCustomerExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastYear);

        assertEquals(1350000L, result);
    }

    @Test
    public void checkVolumeInstrumentWhenNoInstrumentMatch() {

        Rfq rfq1 = new Rfq();
        rfq1.setIsin("ASD");

        String filePath = getClass().getResource("volume-instrument-test.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentByCustomerExtractor extractor = new VolumeTradedForInstrumentByCustomerExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq1, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastWeek);
        Object result1 = meta.get(RfqMetadataFieldNames.volumeInstrumentPastMonth);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeInstrumentPastYear);


        assertEquals(0L, result);
        assertEquals(0L, result1);
        assertEquals(0L, result2);
    }

    @Test
    public void temporaryTest() {

        Rfq rfq2 = new Rfq();
        rfq2.setEntityId(5561279226039690843L);
        rfq2.setIsin("BB0000A0VR22");

        String filePath = getClass().getResource("volume-instrument-test.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        VolumeTradedForInstrumentByCustomerExtractor extractor = new VolumeTradedForInstrumentByCustomerExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq2, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastWeek);
        Object result1 = meta.get(RfqMetadataFieldNames.volumeInstrumentPastMonth);
        Object result2 = meta.get(RfqMetadataFieldNames.volumeInstrumentPastYear);


        assertEquals(0L, result);
        assertEquals(0L, result1);
        assertEquals(100L, result2);
    }

}
