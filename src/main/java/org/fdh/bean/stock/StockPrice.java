package org.fdh.bean.stock;

import lombok.Data;

@Data
public class StockPrice {
    //股票代号
    public String symbol;
    //价格
    public double price;
    //交易时间戳
    public long ts;
    //交易量
    public int volume;
}
