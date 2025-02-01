package feeds

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"coinbot/src/datamodels"
)

type CsvFeed struct {
	filePath                string
	hasHeader               bool
	subscribers             map[string]*datamodels.DataPointSubscription
	mutex                   sync.Mutex
	isStarted               bool
	startTime               time.Time
	endTime                 time.Time
	reader                  *csv.Reader
	file                    *os.File
	currentLine             int64
	buffer                  []datamodels.DataPoint
	bufferCallCount         int
	lastTickTime            time.Time
	totalDataPointsEmitted  int
	schema                  *datamodels.CsvWithTimestampSchema
	coordinator             *FeedCoordinator
	coordinatorReadyChannel chan struct{}
}

type EndOfIntervalError struct {
	Message string
}

func (e *EndOfIntervalError) Error() string {
	return e.Message
}

type CsvFeedBuilder struct {
	filePath  string
	startTime *time.Time
	endTime   *time.Time
	schema    *datamodels.CsvWithTimestampSchema
	hasHeader bool
}

func NewCsvFeedBuilder(filePath string) *CsvFeedBuilder {
	return &CsvFeedBuilder{
		filePath: filePath,
	}
}

func (b *CsvFeedBuilder) WithStartTime(startTime time.Time) *CsvFeedBuilder {
	b.startTime = &startTime
	return b
}

func (b *CsvFeedBuilder) WithEndTime(endTime time.Time) *CsvFeedBuilder {
	b.endTime = &endTime
	return b
}

func (b *CsvFeedBuilder) WithSchema(schema *datamodels.CsvWithTimestampSchema) *CsvFeedBuilder {
	b.schema = schema
	return b
}

func (b *CsvFeedBuilder) WithHasHeader(hasHeader bool) *CsvFeedBuilder {
	b.hasHeader = hasHeader
	return b
}

func (b *CsvFeedBuilder) Build() (*CsvFeed, error) {
	var startTime time.Time
	if b.startTime == nil {
		return nil, fmt.Errorf("start time is required")
	} else {
		startTime = *b.startTime
	}

	var endTime time.Time
	if b.endTime != nil {
		endTime = *b.endTime
		if endTime.Before(startTime) {
			return nil, fmt.Errorf("end time is before start time")
		}
	} else {
		endTime = time.Time{}
	}

	if b.schema == nil {
		return nil, fmt.Errorf("schema is required")
	}

	file, err := os.Open(b.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file at %s: %w", b.filePath, err)
	}

	if b.hasHeader && b.schema != nil {
		slog.Warn("Schema provided but hasHeader is true, ignoring header")
	}

	if b.schema.GetTimestampFieldIndex() == -1 {
		return nil, fmt.Errorf("schema must have timestamp field")
	}

	reader := csv.NewReader(file)

	feed := &CsvFeed{
		filePath:    b.filePath,
		hasHeader:   b.hasHeader,
		subscribers: make(map[string]*datamodels.DataPointSubscription),
		mutex:       sync.Mutex{},
		startTime:   startTime,
		endTime:     endTime,
		reader:      reader,
		file:        file,
		currentLine: 0,
		buffer:      make([]datamodels.DataPoint, 0),
		schema:      b.schema,
	}

	return feed, nil
}

func (c *CsvFeed) GetName() string {
	thisStructName := strings.Split(reflect.TypeOf(c).String(), ".")[1]
	var relationName string
	if c == nil {
		slog.Error("CsvFeed is nil, cannot get name")
		return ""
	}
	if c.schema != nil {
		if c.schema.RelationName == "" {
			// get colnames from schema
			cols := c.schema.GetFieldsOrdered()
			colNames := make([]string, len(cols))
			for i, col := range cols {
				colNames[i] = col.FieldName
			}
			relationName = strings.Join(colNames, "|")
		} else {
			relationName = c.schema.RelationName
		}
	} else {
		// last part of the filepath
		lastSlashIndex := strings.LastIndex(c.filePath, "/")
		relationName = c.filePath[lastSlashIndex+1:]
	}
	return thisStructName + "_" + relationName
}

func (c *CsvFeed) GetOutputFieldNames() []string {
	fieldNames := []string{}
	fields := c.schema.GetFieldsOrdered()
	for _, field := range fields {
		fieldNames = append(fieldNames, field.FieldName)
	}
	return fieldNames
}

func (c *CsvFeed) GetCurrentStreamTime() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.coordinator.GetStreamTime()
}

func (c *CsvFeed) Subscribe(ctx context.Context, subscriberName string) (*datamodels.DataPointSubscription, error) {
	dataChan := make(chan datamodels.DataPoint, 100)
	subscriptionId := uuid.New().String()
	subscription := &datamodels.DataPointSubscription{
		SubscriptionId:   subscriptionId,
		SubscriptionName: fmt.Sprintf("%s_%s", subscriberName, c.GetName()),
		DataPointChan:    dataChan,
		DoneChan:         make(chan struct{}),
		ErrorChan:        make(chan error),
	}

	c.mutex.Lock()
	c.subscribers[subscriptionId] = subscription
	c.mutex.Unlock()

	return subscription, nil
}

func (c *CsvFeed) Unsubscribe(ctx context.Context, subscriptionId string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if subscription, exists := c.subscribers[subscriptionId]; exists {
		close(subscription.DoneChan)
		delete(c.subscribers, subscriptionId)
	}
	return nil
}

func (c *CsvFeed) getSubscribers() map[string]*datamodels.DataPointSubscription {
	if c.mutex.TryLock() {
		defer c.mutex.Unlock()

		subscribers := make(map[string]*datamodels.DataPointSubscription)
		for id, subscription := range c.subscribers {
			subscribers[id] = subscription
		}
		return subscribers
	}

	slog.Error("Failed to get subscribers, mutex already locked, retrying...")
	time.Sleep(100 * time.Millisecond)
	return c.getSubscribers()
}

func (c *CsvFeed) setSubscribers(subscribers map[string]*datamodels.DataPointSubscription) {
	if c.mutex.TryLock() {
		defer c.mutex.Unlock()
		c.subscribers = subscribers
	} else {
		slog.Error("Failed to set subscribers, mutex already locked, retrying...")
		time.Sleep(100 * time.Millisecond)
		c.setSubscribers(subscribers)
	}
}

func (c *CsvFeed) incrementCurrentLine() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.currentLine++
}

func (c *CsvFeed) incrementTotalDataPointsEmitted() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.totalDataPointsEmitted++
}

func (c *CsvFeed) getTotalDataPointsEmitted() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.totalDataPointsEmitted
}

func (c *CsvFeed) getCurrentLine() int64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.currentLine
}

func (c *CsvFeed) addToBuffer(dataPoint datamodels.DataPoint) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.buffer = append(c.buffer, dataPoint)
	c.bufferCallCount++
}

func (c *CsvFeed) getBuffer() []datamodels.DataPoint {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.buffer
}

func (c *CsvFeed) clearBuffer() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.buffer = make([]datamodels.DataPoint, 0)
}

func (c *CsvFeed) getBufferCallCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.bufferCallCount
}

func (c *CsvFeed) getLastTickTime() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.lastTickTime
}

func (c *CsvFeed) seekToStartTime() error {
	slog.Info(fmt.Sprintf("%s is seeking to start time", c.GetName()))
	// Reset file pointer and reader
	if _, err := c.file.Seek(0, 0); err != nil {
		return err
	}

	c.currentLine = 0
	for {
		record, err := c.reader.Read() // reads in min(4096,len(filebytes)) bytes
		if err == io.EOF {
			return fmt.Errorf("reached end of file before finding start time")
		}
		if err != nil {
			return err
		}
		c.incrementCurrentLine()
		if c.hasHeader && c.currentLine == 1 {
			continue
		}
		timestamp, err := strconv.ParseInt(record[c.schema.GetTimestampFieldIndex()], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse timestamp: %w", err)
		}

		recordTime := time.Unix(timestamp, 0)
		if !recordTime.Before(c.startTime) {
			slog.Info(fmt.Sprintf("%s found start time on line %d: %s", c.GetName(), c.currentLine-1, strings.Join(record, ",")))
			// can't seek back, so add the data point to the buffer
			dataPoint, err := c.dataPointFromRecord(record)
			if err != nil {
				return err
			}
			c.addToBuffer(dataPoint)
			return nil
		}
	}
}

func (c *CsvFeed) readDataPointsForNextInterval() ([]datamodels.DataPoint, error) {
	var dataPoints []datamodels.DataPoint

	if c.coordinator == nil {
		return nil, fmt.Errorf("no coordinator set")
	}

	currentTime := c.coordinator.GetStreamTime()
	nextTickTime := currentTime.Add(c.coordinator.GetTickInterval())

	// Check if we've reached the end time
	if !c.endTime.IsZero() && nextTickTime.After(c.endTime) {
		nextTickTime = c.endTime
	}

	// Read data points until we reach the next tick time
	for {
		record, err := c.reader.Read()
		if err == io.EOF {
			return dataPoints, io.EOF
		}
		if err != nil {
			return nil, err
		}

		c.incrementCurrentLine()

		dataPoint, err := c.dataPointFromRecord(record)
		if err != nil {
			return nil, err
		}

		timestamp := dataPoint.GetTimestamp()
		if timestamp.Before(nextTickTime) {
			dataPoints = append(dataPoints, dataPoint)
		} else {
			c.addToBuffer(dataPoint)
			break
		}
	}

	return dataPoints, nil
}

func (c *CsvFeed) Start(ctx context.Context) error {
	if c.isStarted {
		return fmt.Errorf("feed already started")
	}

	if c.coordinator == nil {
		return fmt.Errorf("feed must be added to a coordinator before starting")
	}

	slog.Info("Starting CsvFeed", "feed", c.GetName(), "filePath", c.filePath)
	// Seek to the appropriate starting position
	if err := c.seekToStartTime(); err != nil {
		c.file.Close()
		return fmt.Errorf("failed to seek to start time: %w", err)
	}

	c.lastTickTime = c.startTime

	c.isStarted = true
	go c.streamData(ctx)
	return nil
}

func (c *CsvFeed) IsStarted() bool {
	return c.isStarted
}

func (c *CsvFeed) streamData(ctx context.Context) {
	feedName := c.GetName()
	defer c.file.Close()
	defer c.broadcastDone()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context done, stopping stream")
			return
		default:
			buffer := c.getBuffer()
			c.clearBuffer()
			dataPoints, err := c.readDataPointsForNextInterval()
			if len(buffer) > 0 {
				dataPoints = append(buffer, dataPoints...)
			}
			if err != nil && err != io.EOF {
				slog.Error("Error reading data points", "error", err)
				continue
			}

			// Wait for coordinator to signal next time step
			slog.Debug("Sending feed ready signal", "feedName", feedName)
			c.coordinator.SendFeedReadySignal(feedName)
			<-c.coordinatorReadyChannel

			if err == io.EOF {
				slog.Info(fmt.Sprintf("CsvFeed %s reached end of file, sending last %d data points", feedName, len(dataPoints)))
				c.broadcastDataPoints(dataPoints)
				// wait for one second before sending done signal
				time.Sleep(1 * time.Second)
				c.coordinator.SendFeedDoneSignal(feedName)
				return
			}
			if len(dataPoints) > 0 {
				slog.Debug("Broadcasting data points", "feedName", feedName, "count", len(dataPoints))
				c.broadcastDataPoints(dataPoints)
			} else {
				slog.Warn("No data points read, skipping broadcast")
			}
			continue
		}
	}
}

func (c *CsvFeed) broadcastDataPoints(dataPoints []datamodels.DataPoint) error {
	for _, dataPoint := range dataPoints {
		c.incrementTotalDataPointsEmitted()
		subscribers := c.getSubscribers()
		for _, subscription := range subscribers {
			select {
			// case <-subscription.DoneChan:
			// 	// remove the subscriber
			// 	slog.Info("Subscriber done, removing", "subscriptionName", subscription.SubscriptionName)
			// 	c.Unsubscribe(context.Background(), subscription.SubscriptionId)
			// 	continue
			case subscription.DataPointChan <- dataPoint:
			case <-time.After(time.Second * 1):
				slog.Warn("Feed subscriber to channel full, skipping data point",
					"subscriberName", subscription.SubscriptionName,
					"feedName", c.GetName(),
					"dataPoint", dataPoint)
			}
		}
	}
	return nil
}

func (c *CsvFeed) broadcastDone() {
	// broadcast done to all subscribers
	totalDataPointsEmitted := c.getTotalDataPointsEmitted()
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, subscription := range c.subscribers {
		slog.Info(fmt.Sprintf("Feed %s broadcasting done to %s after %d data points", c.GetName(), subscription.SubscriptionName, totalDataPointsEmitted))
		// broadcast done
		subscription.DoneChan <- struct{}{}
	}
}

func (c *CsvFeed) Stop() error {
	if !c.isStarted {
		return nil
	}

	c.setSubscribers(make(map[string]*datamodels.DataPointSubscription))
	c.isStarted = false

	closeErr := c.file.Close()
	if closeErr != nil {
		if pathErr, ok := closeErr.(*fs.PathError); ok {
			searchString := "file already closed"
			if strings.Contains(pathErr.Error(), searchString) {
				slog.Info("File already closed, feed stopped")
				return nil
			}
		} else {
			slog.Error("Error closing file", "error", closeErr)
		}
	}

	slog.Info(fmt.Sprintf("Feed %s stopped", c.GetName()))
	return nil
}

func (c *CsvFeed) GetDataBetween(ctx context.Context, start time.Time, end time.Time) ([]datamodels.DataPoint, error) {
	// Create a new file handle for this operation to not interfere with streaming
	file, err := os.Open(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	result := make([]datamodels.DataPoint, 0)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		dataPoint, err := c.dataPointFromRecord(record)
		if err != nil {
			return nil, err
		}
		if dataPoint.GetTimestamp().Before(start) {
			continue
		}
		if dataPoint.GetTimestamp().After(end) {
			// TODO: backtrack to the previous data point and return
			break
		}
		result = append(result, dataPoint)
	}

	return result, nil
}

func (c *CsvFeed) dataPointFromRecord(record []string) (datamodels.DataPoint, error) {
	// Parse timestamp using schema
	var timestamp int64
	var err error

	timestampField, err := c.schema.GetFieldByIndex(c.schema.GetTimestampFieldIndex())
	if err != nil {
		return nil, fmt.Errorf("failed to get timestamp field: %w", err)
	}

	switch timestampField.FieldType {
	case datamodels.FieldTypeInt:
		timestamp, err = strconv.ParseInt(record[c.schema.GetTimestampFieldIndex()], 10, 64)
	default:
		timestampField, err := c.schema.GetFieldByIndex(c.schema.GetTimestampFieldIndex())
		if err != nil {
			return nil, fmt.Errorf("failed to get timestamp field: %w", err)
		}
		return nil, fmt.Errorf("unsupported timestamp field type: %s", timestampField.FieldType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	recordTime := time.Unix(timestamp, 0)

	dataPoint := datamodels.GenericDataPoint{
		Timestamp: recordTime,
		Elements:  make([]datamodels.DataPointElement, len(c.schema.Fields)),
	}

	// Parse each field according to schema
	elementIndex := 0
	for i := range record {
		// Skip timestamp field since we already handled it
		if i+1 == c.schema.GetTimestampFieldIndex() {
			continue
		}

		field, err := c.schema.GetFieldByIndex(i)
		if err != nil {
			slog.Error("Error getting field by index", "error", err, "index", i, "schema", c.schema)
			continue
		}

		var value interface{}
		switch field.FieldType {
		case datamodels.FieldTypeFloat:
			value, err = strconv.ParseFloat(record[i], 64)
		case datamodels.FieldTypeString:
			value = record[i]
		case datamodels.FieldTypeInt:
			value, err = strconv.ParseInt(record[i], 10, 64)
		default:
			return nil, fmt.Errorf("unsupported field type: %s", field.FieldType)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to parse field %s: %w", field.FieldName, err)
		}

		dataPoint.Elements[elementIndex] = datamodels.DataPointElement{
			Field:     field.FieldName,
			FieldType: field.FieldType,
			Value:     value,
		}
		elementIndex++
	}

	return &dataPoint, nil
}
