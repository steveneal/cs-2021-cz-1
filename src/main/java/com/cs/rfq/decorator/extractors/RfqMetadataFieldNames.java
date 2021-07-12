package com.cs.rfq.decorator.extractors;

/**
 * Enumeration of all metadata that will be published by this component
 */
public enum RfqMetadataFieldNames {
    tradeSideBiasPastWeek,
    tradeSideBiasPastMonth,
    volumeTradedByEntityPastWeek,
    volumeTradedByEntityPastMonth,
    volumeTradedByEntityPastYear,
    volumeInstrumentPastWeek,
    volumeInstrumentPastMonth,
    volumeInstrumentPastYear,
    averageTradePricePastWeek,
    averageTradeVolumeAveragePricePastWeek,
    liquidity,
    // rfq fields
    id,
    instrumentId,
    traderId,
    entityId,
    qty,
    price,
    side
}
