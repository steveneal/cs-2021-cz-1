package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InstrumentLiquidityExtractorTest extends AbstractSparkUnitTest {
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkLiquidityWhenTradesMatch() {

        String filePath = getClass().getResource("test_liquidity_monthly_match.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastMonth);

        assertEquals(1_350_000L, result);
    }

    @Test
    public void checkLiquidityWhenNoTradesMatch() {

        String filePath = getClass().getResource("test_liquidity_monthly_nomatch.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentLiquidityExtractor extractor = new InstrumentLiquidityExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object result = meta.get(RfqMetadataFieldNames.volumeInstrumentPastMonth);

        assertEquals(0L, result);
    }
}
