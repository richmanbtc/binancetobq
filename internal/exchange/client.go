package exchange

import (
	"net/http"
	"time"
)

type Client struct {
	rest, stream string
	client       *http.Client
	pace         time.Duration
}

type Options struct {
	REST, WebSocket string
	HTTPClient      *http.Client
	Pace            time.Duration
}

func New(o Options) *Client {
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{rest: o.REST, stream: o.WebSocket, client: client, pace: o.Pace}
}
