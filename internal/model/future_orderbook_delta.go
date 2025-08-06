package model

//
// ───────────────────────────────────────────────────────────────
// 📡 Future Order Book Delta Models (WebSocket messages)
// ───────────────────────────────────────────────────────────────
//

// 🔶 Normalized delta format
type FutureOrderBookDelta struct {
    Exchange   string           `json:"exchange"`
    Symbol     string           `json:"symbol"`
    Timestamp  int64            `json:"timestamp"`
    Bids       []OrderBookEntry `json:"bids"`
    Asks       []OrderBookEntry `json:"asks"`
    RawMessage []byte           `json:"raw_message"`
}

// 🔶 Flattened format
type FutureDeltaFlattenedRow struct {
    Timestamp  int64   `json:"timestamp"`
    Exchange   string  `json:"exchange"`
    Symbol     string  `json:"symbol"`
    Side       string  `json:"side"`
    Price      float64 `json:"price"`
    Quantity   float64 `json:"quantity"`
    Level      int     `json:"level"`
    Source     string  `json:"source"` // "delta"
    MarketType string  `json:"market_type"` // "future"
}

// 🔹 Binance WS
type BinanceFutureDelta struct {
    EventType string     `json:"e"`
    EventTime int64      `json:"E"`
    Symbol    string     `json:"s"`
    UpdateID  int64      `json:"u"`
    Bids      [][2]string `json:"b"`
    Asks      [][2]string `json:"a"`
}

// 🔹 Bybit WS
type BybitFutureDelta struct {
    Topic     string `json:"topic"`
    Type      string `json:"type"`
    Timestamp int64  `json:"ts"`
    Data struct {
        Bids [][2]string `json:"b"`
        Asks [][2]string `json:"a"`
    } `json:"data"`
}

// 🔹 OKX WS
type OKXFutureDelta struct {
    Arg struct {
        Channel string `json:"channel"`
        InstID  string `json:"instId"`
    } `json:"arg"`
    Data []struct {
        Asks [][]string `json:"asks"`
        Bids [][]string `json:"bids"`
        Ts   string     `json:"ts"`
    } `json:"data"`
}