package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
public class TradeSideBiasExtractorTest extends AbstractSparkUnitTest {

    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setEntityId(5561279226039690843L);
        rfq.setIsin("AT0000A0VRQ6");
    }


    @Test
    public void checkWeeklyRatioWhenMatch() {

        String filePath = getClass().getResource("test_tradesidebias_weekly_match.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBiasPastWeek);

        assertEquals(1.7, result);
    }

    @Test
    public void checkWeeklyRatioWhenNoMatch() {

        String filePath = getClass().getResource("test_tradesidebias_weekly_nomatch.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBiasPastWeek);

        assertEquals(-1.0, result);
    }

    @Test
    public void checkMonthlyRatioWhenMatch() {

        String filePath = getClass().getResource("test_tradesidebias_monthly_match.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBiasPastMonth);

        assertEquals(1.7, result);
    }

    @Test
    public void checkMonthlyRatioWhenNoMatch() {

        String filePath = getClass().getResource("test_tradesidebias_monthly_nomatch.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        TradeSideBiasExtractor extractor = new TradeSideBiasExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.tradeSideBiasPastMonth);

        assertEquals(-1.0, result);
    }


}