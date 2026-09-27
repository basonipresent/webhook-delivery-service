// Package dispatch delivers webhook attempts and decides what happens next.
package dispatch

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Sign returns hex(HMAC-SHA256(secret, "<ts>.<body>")). ts is Unix seconds and
// body must be the exact bytes sent. Merchants recompute this over the raw
// request body and the Webhook-Timestamp header.
func Sign(secret []byte, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
