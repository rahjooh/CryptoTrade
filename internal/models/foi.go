package models

import (
	"encoding/json"
	"time"
)

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// GENERAL ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// RawFOI is what flows on foi.raw
type RawFOI struct {
	Exchange string          `json:"exchange"`
	Payload  json.RawMessage `json:"payload"` // raw exchange payload
}

// Normalized FOI is the envelope that flows on foi.norm.
//
// Exactly ONE of Binance / Bybit / OKX is non-nil, depending on Exchange.
type NormFOI struct {
	Exchange string    `json:"exchange"`
	Time     time.Time `json:"time"`

	Binance *BinanceNormFOI     `json:"binance,omitempty"`
	Bybit   *BybitNormalizedFOI `json:"bybit,omitempty"`
	OKX     *OKXNormalizedFOI   `json:"okx,omitempty"`
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BINANCE ///////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BinanceFOICurrentResp (USDⓈ-M / COIN-M) current OI (e.g., /fapi/v1/openInterest)
type BinanceFOICurrentResp struct {
	Symbol       string `json:"symbol"`
	OpenInterest string `json:"openInterest"`
	Time         int64  `json:"time"` // event/response ts (ms)
}

// BinanceNormFOI is normalized FOI specifically for Binance.
type BinanceNormFOI struct {
	Symbol       string  `json:"symbol"`
	EventTimeMs  int64   `json:"event_time_ms"` // exchange event/response time (ms)
	OpenInterest float64 `json:"open_interest"` // contracts
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////////// BYBIT /////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// BybitFOIOpenInterestResponse matches /v5/market/open-interest
//
// GET /v5/market/open-interest
//   ?category=linear
//   &symbol=BTCUSDT
//   &intervalTime=5min
//   &limit=200
//
// {
//   "retCode": 0,
//   "retMsg": "OK",
//   "result": {
//     "symbol": "BTCUSDT",
//     "category": "linear",
//     "list": [
//       {
//         "openInterest": "123456.78900000",
//         "timestamp": "1669571400000"
//       }
//     ],
//     "nextPageCursor": ""
//   },
//   "retExtInfo": {},
//   "time": 1672053548579
// }

// BybitFOIResp (e.g., /derivatives/v3/public/open-interest)
type BybitFOIOpenInterestResponse struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Symbol   string `json:"symbol"`
		Category string `json:"category"`
		List     []struct {
			OpenInterest string `json:"openInterest"`
			Timestamp    string `json:"timestamp"` // ms as string
		} `json:"list"`
		NextPageCursor string `json:"nextPageCursor"`
	} `json:"result"`
	RetExtInfo map[string]any `json:"retExtInfo"`
	Time       int64          `json:"time"`
}

// BybitNormalizedFOI – normalized FOI for Bybit.
type BybitNormalizedFOI struct {
	Symbol       string  `json:"symbol"`
	Category     string  `json:"category"`
	Interval     string  `json:"interval"`      // e.g. "5min"
	EventTimeMs  int64   `json:"event_time_ms"` // from list.timestamp
	OpenInterest float64 `json:"open_interest"` // contracts
}

///////////////////////////////////////////////////////////////////////////////
////////////////////////////////// OKX FOI ////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

// OKXFOIEvent is the websocket payload we care about for open interest.
//
// Example:
//
//	{
//	  "arg": {
//	    "channel": "open-interest",
//	    "instId": "BTC-USDT-SWAP"
//	  },
//	  "data": [
//	    {
//	      "instId": "BTC-USDT-SWAP",
//	      "instType": "SWAP",
//	      "oi": "2216113.01000000309",
//	      "oiCcy": "22161.1301000000309",
//	      "oiUsd": "1939251795.54769270396321",
//	      "ts": "1743041250440"
//	    }
//	  ]
//	}
type OKXFOIEvent struct {
	Arg  OKXFOIArg      `json:"arg"`
	Data []OKXFOIRecord `json:"data"`
}

type OKXFOIArg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

type OKXFOIRecord struct {
	InstID   string `json:"instId"`
	InstType string `json:"instType"`
	OI       string `json:"oi"`    // contracts
	OICcy    string `json:"oiCcy"` // coin-denominated OI
	OIUsd    string `json:"oiUsd"` // USD-equivalent OI
	Ts       string `json:"ts"`    // ms as string
}

// OKXNormalizedFOI is the normalized open-interest row for OKX, referenced
// from NormFOI.OKX. This lets the writer map OKX FOI into its own Parquet
// schema, independent from Binance/Bybit.
type OKXNormalizedFOI struct {
	InstID      string  `json:"inst_id"`
	InstType    string  `json:"inst_type"`
	OI          float64 `json:"oi"`
	OICcy       float64 `json:"oi_ccy"`
	OIUsd       float64 `json:"oi_usd"`
	EventTimeMs int64   `json:"event_time_ms"`
}
