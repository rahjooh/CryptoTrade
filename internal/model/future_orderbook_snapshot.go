package model

//
// ───────────────────────────────────────────────────────────────
// 📦 Future Order Book Snapshot Models (REST responses)
// ───────────────────────────────────────────────────────────────
//

// 🔶 Normalized snapshot format (internal use)
type FutureOrderBookSnapshot struct {
	Exchange   string           `json:"exchange"`
	Symbol     string           `json:"symbol"`
	Timestamp  int64            `json:"timestamp"`
	Bids       []OrderBookEntry `json:"bids"`
	Asks       []OrderBookEntry `json:"asks"`
	RawMessage []byte           `json:"raw_message"`
}

// 🔶 Flattened row format (for analytics, persistence)
type FutureSnapshotFlattenedRow struct {
	Timestamp  int64   `json:"timestamp"`
	Exchange   string  `json:"exchange"`
	Symbol     string  `json:"symbol"`
	Side       string  `json:"side"`
	Price      float64 `json:"price"`
	Quantity   float64 `json:"quantity"`
	Level      int     `json:"level"`
	Source     string  `json:"source"`      // "snapshot"
	MarketType string  `json:"market_type"` // "future"
}

// 🔹 Binance Future REST Response
type BinanceFutureSnapshot struct {
	LastUpdateID int64        `json:"lastUpdateId"`
	Bids         [][2]string  `json:"bids"`
	Asks         [][2]string  `json:"asks"`
}

// 🔹 Bybit Future REST Response
type BybitFutureSnapshot struct {
	RetCode int    `json:"retCode"`
	RetMsg  string `json:"retMsg"`
	Result  struct {
		Symbol string      `json:"s"`
		Bids   [][2]string `json:"b"`
		Asks   [][2]string `json:"a"`
		Ts     int64       `json:"ts"`
		U      int64       `json:"u"`
	} `json:"result"`
	Time int64 `json:"time"`
}

// 🔹 OKX Future REST Response
type OKXFutureSnapshot struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		Asks [][]string `json:"asks"`
		Bids [][]string `json:"bids"`
		Ts   string     `json:"ts"`
	} `json:"data"`
}
