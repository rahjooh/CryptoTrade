package bybit

type bybitTickerEntry struct {
	Symbol          string `json:"symbol"`
	MarkPrice       string `json:"markPrice"`
	IndexPrice      string `json:"indexPrice"`
	OpenInterest    string `json:"openInterest"`
	FundingRate     string `json:"fundingRate"`
	NextFundingTime string `json:"nextFundingTime"`
}

type bybitTickerPayload struct {
	Topic string             `json:"topic"`
	Type  string             `json:"type"`
	Ts    int64              `json:"ts"`
	Data  []bybitTickerEntry `json:"data"`
}
