package report

import (
	"fmt"
	"time"
)

// Keep the full Slack timestamp: truncating seconds includes messages outside
// the advertised window when a boundary falls within a second.
const messageWindowSQL = `m.ts not like 'draft:%'
  and instr(m.ts, '.') > 0
  and m.ts >= ?
  and m.ts <= ?`

// Some sources omit thread_ts on roots with reply metadata. Count rows, since
// two channels may have roots with the same timestamp.
const threadCountSQL = `count(case when m.thread_ts = m.ts
  or (coalesce(m.thread_ts, '') = '' and m.reply_count > 0) then 1 end)`

func slackTSBoundary(t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%d.%06d", t.Unix(), t.Nanosecond()/1000)
}

func slackTSLowerBound(t time.Time) string {
	// Slack timestamps have microsecond precision; round a lower bound up so
	// a message just before a nanosecond-resolution clock is not included.
	if remainder := time.Duration(t.Nanosecond()) % time.Microsecond; remainder != 0 {
		t = t.Add(time.Microsecond - remainder)
	}
	return slackTSBoundary(t)
}
