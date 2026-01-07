package service

import (
	"chat/logic/repo"
	"database/sql"
	"log"
	"time"
)

type AsyncWriter struct {
	msgChan chan *WrappedMessage
	done    chan struct{}
}

type WrappedMessage struct {
	RepoMsg   *repo.Message
	OnSuccess func()
	OnFailure func()
}

const (
	batchSize     = 100                    // Accumulate up to 100 messages
	flushInterval = 500 * time.Millisecond // Or flush every 500ms
)

var GlobalWriter *AsyncWriter

func StartAsyncWriter() {
	GlobalWriter = &AsyncWriter{
		msgChan: make(chan *WrappedMessage, 10000), // Buffer size
		done:    make(chan struct{}),
	}
	go GlobalWriter.run()
}

func (w *AsyncWriter) run() {
	buffer := make([]*WrappedMessage, 0, batchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}

		// Extract repo messages
		repoMsgs := make([]*repo.Message, len(buffer))
		for i, wm := range buffer {
			repoMsgs[i] = wm.RepoMsg
		}

		if err := repo.BatchSaveMessages(repoMsgs); err != nil {
			log.Printf("ERROR: Failed to batch save messages: %v. NACKing batch.", err)
			// On Failure: NACK all
			for _, wm := range buffer {
				if wm.OnFailure != nil {
					wm.OnFailure()
				}
			}
		} else {
			log.Printf("Batched saved %d messages. ACKing batch.", len(buffer))
			// On Success: ACK all
			for _, wm := range buffer {
				if wm.OnSuccess != nil {
					wm.OnSuccess()
				}
			}
		}
		buffer = buffer[:0] // Clear buffer
	}

	for {
		select {
		case msg := <-w.msgChan:
			buffer = append(buffer, msg)
			if len(buffer) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-w.done:
			flush()
			return
		}
	}
}

func (w *AsyncWriter) Stop() {
	close(w.done)
}

func (w *AsyncWriter) Push(msg *repo.Message, onSuccess, onFailure func()) {
	wrapped := &WrappedMessage{
		RepoMsg:   msg,
		OnSuccess: onSuccess,
		OnFailure: onFailure,
	}
	select {
	case w.msgChan <- wrapped:
	default:
		log.Println("WARNING: AsyncWriter channel full, dropping message (NACKing)!")
		if onFailure != nil {
			onFailure()
		}
	}
}

// Helper to convert args
func AsyncSaveMessage(fromUserID int64, toUserID sql.NullInt64, toGroupID sql.NullInt64, content, msgType string, onSuccess, onFailure func()) {
	msg := &repo.Message{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		ToGroupID:  toGroupID,
		Content:    content,
		Type:       msgType,
	}
	if GlobalWriter != nil {
		GlobalWriter.Push(msg, onSuccess, onFailure)
	} else {
		// Fallback or Error if writer not started
		log.Println("AsyncWriter not started, executing onFailure")
		if onFailure != nil {
			onFailure()
		}
	}
}
