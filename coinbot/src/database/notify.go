package database

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"coinbot/src/utils/errors"
)

type NotificationManager struct {
	db          *gorm.DB
	listener    *pq.Listener
	subscribers map[string]map[string]map[string]chan<- string
	mu          sync.RWMutex
}

func NewNotificationManager(db *gorm.DB) (*NotificationManager, error) {
	connStr := db.Config.Dialector.(*postgres.Dialector).DSN
	listener := pq.NewListener(connStr, 10*time.Second, time.Minute, nil)

	nm := &NotificationManager{
		db:          db,
		listener:    listener,
		subscribers: make(map[string]map[string]map[string]chan<- string), // channel -> objectID -> subscriberID -> chan
	}

	go nm.listen()

	return nm, nil
}

func (nm *NotificationManager) listen() {
	for notification := range nm.listener.Notify {
		if notification == nil {
			continue
		}
		nm.handleNotification(notification.Channel, notification.Extra)
	}
}

func (nm *NotificationManager) handleNotification(channel, payload string) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	objectId, msg, ok := strings.Cut(payload, ";")
	if !ok {
		slog.Error("Invalid payload format", "payload", payload)

		return
	}

	if subs, ok := nm.subscribers[channel]; ok {
		if objSubs, ok := subs[objectId]; ok {
			for _, ch := range objSubs {
				select {
				case ch <- msg:
					slog.Info("Notification sent", "channel", channel, "objectID", objectId, "payload", msg)
				default:
					slog.Warn("Notification channel is full, skipping", "channel", channel)
				}
			}
		}
	}
}

func (nm *NotificationManager) Subscribe(ctx context.Context, subscriberID string, objectType string, objectID string) (<-chan string, error) {
	channel := objectType
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if _, ok := nm.subscribers[channel]; !ok {
		if err := nm.listener.Listen(channel); err != nil {
			return nil, errors.Wrapf(err, "failed to listen on channel %s", channel)
		}
		nm.subscribers[channel] = make(map[string]map[string]chan<- string)
	}

	if nm.subscribers[channel][objectID] == nil {
		nm.subscribers[channel][objectID] = make(map[string]chan<- string)
	}

	ch := make(chan string, 10)
	nm.subscribers[channel][objectID][subscriberID] = ch

	slog.Info("Subscribed to channel", "channel", channel, "objectID", objectID, "subscriberID", subscriberID)
	return ch, nil
}

func (nm *NotificationManager) NewSubscriber(ctx context.Context) string {
	uuid := uuid.New()
	return uuid.String()
}

func (nm *NotificationManager) Unsubscribe(objectType string, subscriberID string, objectIDs ...string) error {
	channel := objectType

	nm.mu.Lock()
	defer nm.mu.Unlock()

	subs, ok := nm.subscribers[channel]
	if !ok {
		return errors.Newf("no subscribers for channel %s", channel)
	}

	for _, objectID := range objectIDs {
		if objSubs, ok := subs[objectID]; ok {
			if ch, exists := objSubs[subscriberID]; exists {
				close(ch)
				delete(objSubs, subscriberID)
			}

			if len(objSubs) == 0 {
				delete(subs, objectID)
			}
		}
	}

	if len(subs) == 0 {
		err := nm.listener.Unlisten(channel)
		if err != nil {
			return errors.Wrapf(err, "failed to unlisten on channel %s", channel)
		}
		delete(nm.subscribers, channel)
	}

	return nil
}

func (nm *NotificationManager) Close() error {
	nm.listener.UnlistenAll()

	return nm.listener.Close()
}

// Add this new method to the NotifyManager struct

func Notify(db *gorm.DB, objectType string, objectID string, payload string) error {
	channel := objectType
	msg := objectID + ";" + payload

	_, err := db.Raw("SELECT pg_notify(?, ?)", channel, msg).Rows()
	if err != nil {
		return errors.Wrapf(err, "failed to send notification")
	}

	return nil
}

func FanIn(ctx context.Context, channels ...<-chan string) <-chan string {
	out := make(chan string) // Output channel
	var wg sync.WaitGroup

	for _, ch := range channels {
		if ch == nil { // Skip nil channels
			continue
		}
		wg.Add(1)
		go func(c <-chan string) {
			defer wg.Done()
			for {
				select {
				case n, ok := <-c:
					if !ok {
						return
					}
					select {
					case out <- n:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
