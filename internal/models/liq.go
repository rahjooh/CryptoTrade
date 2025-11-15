package models

import (
	"encoding/json"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// GENERAL ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const (
	ExchangeBinance = "binance"
	ExchangeBybit   = "bybit"
	ExchangeOKX     = "okx"
)

// RawLiquidation is what flows on liq.raw
type RawLiquidation struct {
	Exchange string          `json:"exchange"`
	Payload  json.RawMessage `json:"payload"` // raw exchange payload
}

// NormalizedLiquidation is the envelope that flows on liq.norm.
//
// Exactly ONE of Binance / Bybit / OKX is non-nil, depending on Exchange.
type NormalizedLiquidation struct {
	Exchange string    `json:"exchange"`
	Time     time.Time `json:"time"`

	Binance *BinanceNormalizedLiquidation `json:"binance,omitempty"`
	Bybit   *BybitNormalizedLiquidation   `json:"bybit,omitempty"`
	OKX     *OKXNormalizedLiquidation     `json:"okx,omitempty"`
}

//// ---------------- NORMALIZED ----------------
//type NormalizedLiquidation struct {
//	Exchange     string    `json:"exchange"`
//	Symbol       string    `json:"symbol"`
//	Side         string    `json:"side"`
//	PositionSide string    `json:"position_side"`
//	OrderType    string    `json:"order_type"`
//	Time         time.Time `json:"time"`
//	Quantity     float64   `json:"quantity"`
//	Price        float64   `json:"price"`
//	AvgPrice     float64   `json:"avg_price"`
//	LastQty      float64   `json:"last_qty"`
//	LastPrice    float64   `json:"last_price"`
//	TradeID      int64     `json:"trade_id"`
//	IsMaker      bool      `json:"is_maker"`
//	IsReduceOnly bool      `json:"is_reduce_only"`
//	WorkingType  string    `json:"working_type"`
//	OriginalType string    `json:"original_type"`
//	CloseAll     bool      `json:"close_all"`
//	RealizedPnl  float64   `json:"realized_pnl"`
//	//RawJSON       []byte    `json:"raw_json"`
//}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BINANCE ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// ---------------- BINANCE ----------------
//
// {
//   "e": "forceOrder",
//   "E": 1568014460893,
//   "o": { ... }
// }

type BinanceLiquidationEvent struct {
	EventType string                     `json:"e"`
	EventTime int64                      `json:"E"`
	Order     BinanceLiquidationOrderRaw `json:"o"`
}

type BinanceLiquidationOrderRaw struct {
	Symbol       string `json:"s"`
	Side         string `json:"S"`
	OrderType    string `json:"o"`
	TimeInForce  string `json:"f"`
	QuantityStr  string `json:"q"`
	PriceStr     string `json:"p"`
	AvgPriceStr  string `json:"ap"`
	Status       string `json:"X"`
	LastQtyStr   string `json:"l"`
	TotalQtyStr  string `json:"z"`
	TradeTime    int64  `json:"T"`
	LastPriceStr string `json:"L"`
	TradeID      int64  `json:"t"`
	BidNotional  string `json:"b"`
	AskNotional  string `json:"a"`
	Maker        bool   `json:"m"`
	ReduceOnly   bool   `json:"R"`
	WorkingType  string `json:"wt"`
	OriginalType string `json:"ot"`
	PositionSide string `json:"ps"`
	CloseAll     bool   `json:"cp"`
	RealizedPnl  string `json:"rp"`
}

// BinanceNormalizedLiquidation is the normalized shape specifically for Binance forceOrder.
type BinanceNormalizedLiquidation struct {
	Symbol       string `json:"symbol"`
	Side         string `json:"side"`
	PositionSide string `json:"position_side"`
	OrderType    string `json:"order_type"`

	Quantity     float64 `json:"quantity"`
	Price        float64 `json:"price"`
	AvgPrice     float64 `json:"avg_price"`
	LastQty      float64 `json:"last_qty"`
	LastPrice    float64 `json:"last_price"`
	TradeID      int64   `json:"trade_id"`
	IsMaker      bool    `json:"is_maker"`
	IsReduceOnly bool    `json:"is_reduce_only"`
	WorkingType  string  `json:"working_type"`
	OriginalType string  `json:"original_type"`
	CloseAll     bool    `json:"close_all"`
	RealizedPnl  float64 `json:"realized_pnl"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BYBIT /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// Incoming Bybit payload:
//
// {
//   "topic": "allLiquidation.ROSEUSDT",
//   "type": "snapshot",
//   "ts": 1739502303204,
//   "data": [
//     {
//       "T": 1739502302929,
//       "s": "ROSEUSDT",
//       "S": "Sell",
//       "v": "20000",
//       "p": "0.04499"
//     }
//   ]
// }

type BybitLiquidationEvent struct {
	Topic string                   `json:"topic"`
	Type  string                   `json:"type"`
	Ts    int64                    `json:"ts"`
	Data  []BybitLiquidationRecord `json:"data"`
}

type BybitLiquidationRecord struct {
	EventTime int64  `json:"T"` // ms
	Symbol    string `json:"s"`
	Side      string `json:"S"` // "Buy" or "Sell"
	SizeStr   string `json:"v"`
	PriceStr  string `json:"p"`
}

// BybitNormalizedLiquidation is the normalized shape for Bybit allLiquidation.
type BybitNormalizedLiquidation struct {
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"`
	Quantity float64 `json:"quantity"`
	Price    float64 `json:"price"`
}

/////////////////////////////////////////////////////////////////////////////
////////////////////////////////// OKX //////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
// ---------------- OKX ----------------
//
// {
//   "arg": {
//     "channel": "liquidation-orders",
//     "instType": "SWAP"
//   },
//   "data": [
//     {
//       "details": [
//         {
//           "bkLoss": "0",
//           "bkPx": "1.057",
//           "ccy": "",
//           "posSide": "long",
//           "side": "sell",
//           "sz": "768",
//           "ts": "1723892524781"
//         }
//       ],
//       "instFamily": "DYDX-USDT",
//       "instId": "DYDX-USDT-SWAP",
//       "instType": "SWAP",
//       "uly": "DYDX-USDT"
//     }
//   ]
// }

type OKXLiquidationEvent struct {
	Arg  OKXLiquidationArg     `json:"arg"`
	Data []OKXLiquidationBlock `json:"data"`
}

type OKXLiquidationArg struct {
	Channel  string `json:"channel"`
	InstType string `json:"instType"`
}

type OKXLiquidationBlock struct {
	Details    []OKXLiquidationDetail `json:"details"`
	InstFamily string                 `json:"instFamily"`
	InstID     string                 `json:"instId"`
	InstType   string                 `json:"instType"`
	Uly        string                 `json:"uly"`
}

type OKXLiquidationDetail struct {
	BkLoss  string `json:"bkLoss"`
	BkPx    string `json:"bkPx"`
	Ccy     string `json:"ccy"`
	PosSide string `json:"posSide"` // long / short
	Side    string `json:"side"`    // buy / sell
	Sz      string `json:"sz"`
	Ts      string `json:"ts"` // ms (string)
}

// OKXNormalizedLiquidation is the normalized shape for OKX liquidation-orders SWAP.
type OKXNormalizedLiquidation struct {
	Symbol       string  `json:"symbol"`
	Side         string  `json:"side"`
	PositionSide string  `json:"position_side"`
	Quantity     float64 `json:"quantity"`
	Price        float64 `json:"price"`
}
