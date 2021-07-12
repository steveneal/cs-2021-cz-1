package com.cs.rfq.decorator;

import com.cs.rfq.decorator.extractors.RfqMetadataFieldNames;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static com.cs.rfq.decorator.extractors.RfqMetadataFieldNames.instrumentId;

public class Rfq implements Serializable {
    private String id;
    @SerializedName("instrumentId")
    private String isin;
    private Long traderId;
    private Long entityId;
    @SerializedName("qty")
    private Long quantity;
    private Double price;
    private String side;

    public static Rfq fromJson(String json) throws JsonSyntaxException {
        Gson g = new Gson();
        return g.fromJson(json, Rfq.class);
    }

    public HashMap<RfqMetadataFieldNames, Object> toMap() {
        final HashMap<RfqMetadataFieldNames, Object> map = new HashMap<>();
        map.put(RfqMetadataFieldNames.id, this.id);
        map.put(RfqMetadataFieldNames.instrumentId, this.isin);
        map.put(RfqMetadataFieldNames.traderId, this.traderId);
        map.put(RfqMetadataFieldNames.entityId, this.entityId);
        map.put(RfqMetadataFieldNames.qty, this.quantity);
        map.put(RfqMetadataFieldNames.price, this.price);
        map.put(RfqMetadataFieldNames.side, this.side);
        return map;
    }


    @Override
    public String toString() {
        return "Rfq{" +
                "id='" + id + '\'' +
                ", isin='" + isin + '\'' +
                ", traderId=" + traderId +
                ", entityId=" + entityId +
                ", quantity=" + quantity +
                ", price=" + price +
                ", side=" + side +
                '}';
    }

    public boolean isBuySide() {
        return "B".equals(side);
    }

    public boolean isSellSide() {
        return "S".equals(side);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIsin() {
        return isin;
    }

    public void setIsin(String isin) {
        this.isin = isin;
    }

    public Long getTraderId() {
        return traderId;
    }

    public void setTraderId(Long traderId) {
        this.traderId = traderId;
    }

    public Long getEntityId() {
        return entityId;
    }

    public void setEntityId(Long entityId) {
        this.entityId = entityId;
    }

    public Long getQuantity() {
        return quantity;
    }

    public void setQuantity(Long quantity) {
        this.quantity = quantity;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getSide() {
        return side;
    }

    public void setSide(String side) {
        this.side = side;
    }
}
