package model

//
// ───────────────────────────────────────────────────────────────
// 📡 Spot Order Book Delta Models (WebSocket messages)
// ───────────────────────────────────────────────────────────────
//

// 🔶 Normalized delta format
type SpotOrderBookDelta struct {
    Exchange   string           `json:"exchange"`
    Symbol     string           `json:"symbol"`
    Timestamp  int64            `json:"timestamp"`
    Bids       []OrderBookEntry `json:"bids"`
    Asks       []OrderBookEntry `json:"asks"`
    RawMessage []byte           `json:"raw_message"`
}

// 🔶 Flattened format
type SpotDeltaFlattenedRow struct {
    Timestamp  int64   `json:"timestamp"`
    Exchange   string  `json:"exchange"`
    Symbol     string  `json:"symbol"`
    Side       string  `json:"side"` // "bid" or "ask"
    Price      float64 `json:"price"`
    Quantity   float64 `json:"quantity"`
    Level      int     `json:"level"`
    Source     string  `json:"source"` // "delta"
    MarketType string  `json:"market_type"` // "spot"
}

// 🔹 Binance WS
type BinanceSpotDelta struct {
    EventType string     `json:"e"`
    EventTime int64      `json:"E"`
    Symbol    string     `json:"s"`
    UpdateID  int64      `json:"u"`
    Bids      [][2]string `json:"b"`
    Asks      [][2]string `json:"a"`
}

// 🔹 Bybit WS
type BybitSpotDelta struct {
    Topic     string `json:"topic"`
    Type      string `json:"type"`
    Timestamp int64  `json:"ts"`
    Data struct {
        Bids [][2]string `json:"b"`
        Asks [][2]string `json:"a"`
    } `json:"data"`
}

// 🔹 OKX WS
type OKXSpotDelta struct {
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

// 🔹 KuCoin WS
type KucoinSpotDelta struct {
    Type  string `json:"type"`
    Topic string `json:"topic"`
    Data struct {
        SequenceStart int64        `json:"sequenceStart"`
        SequenceEnd   int64        `json:"sequenceEnd"`
        Bids          [][2]string  `json:"bids"`
        Asks          [][2]string  `json:"asks"`
        Timestamp     int64        `json:"timestamp"`
    } `json:"data"`
}

// 🔹 Coinbase WS
type CoinbaseSpotDelta struct {
    Type     string      `json:"type"`
    Product  string      `json:"product_id"`
    Changes  [][3]string `json:"changes"`
    Time     string      `json:"time"`
}

// 🔹 Kraken WS
type KrakenSpotDelta struct {
    ChannelID int              `json:"channelID"`
    Asks      [][]interface{}  `json:"a,omitempty"`
    Bids      [][]interface{}  `json:"b,omitempty"`
}