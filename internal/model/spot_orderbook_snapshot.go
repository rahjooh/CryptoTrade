package model

//
// ───────────────────────────────────────────────────────────────
// 📦 Spot Order Book Snapshot Models (REST responses)
// ───────────────────────────────────────────────────────────────
//

// 🔶 Normalized snapshot format (internal use)
type SpotOrderBookSnapshot struct {
	Exchange   string           `json:"exchange"`
	Symbol     string           `json:"symbol"`
	Timestamp  int64            `json:"timestamp"` // epoch ms
	Bids       []OrderBookEntry `json:"bids"`
	Asks       []OrderBookEntry `json:"asks"`
	RawMessage []byte           `json:"raw_message"`
}

// 🔶 Flattened row format (for analytics, persistence)
type SpotSnapshotFlattenedRow struct {
	Timestamp  int64   `json:"timestamp"`
	Exchange   string  `json:"exchange"`
	Symbol     string  `json:"symbol"`
	Side       string  `json:"side"` // "bid" or "ask"
	Price      float64 `json:"price"`
	Quantity   float64 `json:"quantity"`
	Level      int     `json:"level"`
	Source     string  `json:"source"`      // "snapshot"
	MarketType string  `json:"market_type"` // "spot"
}

// 🔹 Binance Spot REST Response
type BinanceSpotSnapshot struct {
	LastUpdateID int64       `json:"lastUpdateId"`
	Bids         [][2]string `json:"bids"`
	Asks         [][2]string `json:"asks"`
}

// 🔹 Bybit Spot REST Response
type BybitSpotSnapshot struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Symbol string      `json:"s"`
		Bids   [][2]string `json:"b"`
		Asks   [][2]string `json:"a"`
		Ts     int64       `json:"ts"`
	} `json:"result"`
	Time int64 `json:"time"`
}

// 🔹 OKX Spot REST Response
type OKXSpotSnapshot struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
		Ts   string     `json:"ts"`
	} `json:"data"`
}

// 🔹 KuCoin Spot REST Response
type KucoinSpotSnapshot struct {
	Code string `json:"code"`
	Data struct {
		Time int64       `json:"time"`
		Bids [][2]string `json:"bids"`
		Asks [][2]string `json:"asks"`
	} `json:"data"`
}

// 🔹 Coinbase Spot REST Response
type CoinbaseSpotSnapshot struct {
	Sequence int64       `json:"sequence"`
	Bids     [][3]string `json:"bids"`
	Asks     [][3]string `json:"asks"`
}

// 🔹 Kraken Spot REST Response
type KrakenSpotSnapshot struct {
	Result map[string]struct {
		Asks [][]interface{} `json:"asks"`
		Bids [][]interface{} `json:"bids"`
	} `json:"result"`
	Error []string `json:"error"`
}
