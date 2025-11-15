package bybit

import (
	"context"
	"fmt"
	"time"

	"cryptoflow/logger"
	"github.com/gorilla/websocket"
)

const (
	defaultReconnectDelay = 5 * time.Second
	defaultKeepAlive      = 20 * time.Second
)

type bybitSubscriptionAck struct {
	Op      string `json:"op"`
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

func runBybitWebSocket(ctx context.Context, url string, topics []string, reconnectDelay time.Duration, log *logger.Entry, handler func(string) error, onConn func(*websocket.Conn)) {
	if reconnectDelay <= 0 {
		reconnectDelay = defaultReconnectDelay
	}
	dialer := websocket.DefaultDialer
	for {
		if ctx.Err() != nil {
			return
		}

		conn, _, err := dialer.DialContext(ctx, url, nil)
		if err != nil {
			log.WithError(err).WithField("url", url).Warn("failed to connect to bybit websocket")
			if waitForReconnect(ctx, reconnectDelay) {
				return
			}
			continue
		}
		if onConn != nil {
			onConn(conn)
		}

		if len(topics) > 0 {
			if err := subscribeBybit(conn, topics); err != nil {
				log.WithError(err).WithField("url", url).Warn("failed to subscribe to bybit topics")
				if onConn != nil {
					onConn(nil)
				}
				conn.Close()
				if waitForReconnect(ctx, reconnectDelay) {
					return
				}
				continue
			}
		}

		pingCancel := startPingLoop(ctx, conn, defaultKeepAlive, log)

		if err := readMessages(ctx, conn, handler); err != nil && ctx.Err() == nil {
			log.WithError(err).WithField("url", url).Warn("bybit websocket read loop ended")
		}

		if pingCancel != nil {
			pingCancel()
		}

		if onConn != nil {
			onConn(nil)
		}
		conn.Close()

		if ctx.Err() != nil {
			return
		}

		if waitForReconnect(ctx, reconnectDelay) {
			return
		}
	}
}

func subscribeBybit(conn *websocket.Conn, topics []string) error {
	req := struct {
		Op    string   `json:"op"`
		Args  []string `json:"args"`
		ReqID string   `json:"req_id"`
	}{
		Op:    "subscribe",
		Args:  topics,
		ReqID: fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	return conn.WriteJSON(req)
}

func readMessages(ctx context.Context, conn *websocket.Conn, handler func(string) error) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if handler != nil {
			_ = handler(string(msg))
		}
	}
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		delay = defaultReconnectDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return true
	case <-timer.C:
		return false
	}
}

func startPingLoop(ctx context.Context, conn *websocket.Conn, interval time.Duration, log *logger.Entry) context.CancelFunc {
	if interval <= 0 {
		interval = defaultKeepAlive
	}
	pingCtx, cancel := context.WithCancel(ctx)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(time.Second))
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
					log.WithError(err).Warn("failed to send websocket ping")
					cancel()
					return
				}
			}
		}
	}()
	return cancel
}
