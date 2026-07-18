# ASR Go SDK

Resilient Go client for the ASR gateway (`asr.v1`).

```bash
go get github.com/harshithgowdakt/speech/sdks/go
```

```go
import asr "github.com/harshithgowdakt/speech/sdks/go"

c := asr.New(asr.Options{
    URL:    "ws://localhost:8080/v1/stream",
    Config: asr.Config{Encoding: asr.EncodingLINEAR16, SampleRateHz: 16000, LanguageCode: "en-US", InterimResults: true},
    OnTranscript: func(t asr.Transcript) { /* ... */ },
})
if err := c.Start(ctx); err != nil { panic(err) }
c.SendAudio(pcmChunk)
c.Stop(ctx) // waits for final transcripts
```

Auto-reconnect (backoff + immediate on `going_away`) and a replay buffer for
lossless resume are on by default; tune via `Options`. Run the example:

```bash
go run ./sdks/go/example -file audio.raw
```
