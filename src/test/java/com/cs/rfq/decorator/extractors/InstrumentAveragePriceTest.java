package com.cs.rfq.decorator.extractors;

import com.cs.rfq.decorator.Rfq;
import com.cs.rfq.decorator.TradeDataLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InstrumentAveragePriceTest extends AbstractSparkUnitTest {
    private Rfq rfq;

    @Before
    public void setup() {
        rfq = new Rfq();
        rfq.setIsin("AT0000A0VRQ6");
    }

    @Test
    public void checkAverageWhenAllTradesMatch() {

        String filePath = getClass().getResource("volume-traded-2.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        InstrumentAveragePriceExtractor extractor = new InstrumentAveragePriceExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultAvgPrice = meta.get(RfqMetadataFieldNames.averageTradePricePastWeek);
        Object resultVolumeAvgPrice = meta.get(RfqMetadataFieldNames.averageTradeVolumeAveragePricePastWeek);

        assertEquals(134.01433333333335, resultAvgPrice);
        assertEquals(134.01433333333335, resultVolumeAvgPrice);
    }

    @Test
    public void checkAverageWhenNoTradesMatch() {

        String filePath = getClass().getResource("volume-traded-3.json").getPath();
        Dataset<Row> trades = new TradeDataLoader().loadTrades(session, filePath);

        //all test trade data are for 2018 so this will cause no matches
        InstrumentAveragePriceExtractor extractor = new InstrumentAveragePriceExtractor();

        Map<RfqMetadataFieldNames, Object> meta = extractor.extractMetaData(rfq, session, trades);

        Object resultAvgPrice = meta.get(RfqMetadataFieldNames.averageTradePricePastWeek);
        Object resultVolumeAvgPrice = meta.get(RfqMetadataFieldNames.averageTradeVolumeAveragePricePastWeek);

        assertEquals(0.0, resultAvgPrice);
        assertEquals(0.0, resultVolumeAvgPrice);

    }


}
