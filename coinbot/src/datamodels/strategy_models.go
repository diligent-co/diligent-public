package datamodels

import "time"

type SignalType string

const (
	SignalTypeAllocation SignalType = "allocation"
)

type Signal struct {
	SignalId        string
	StrategyId      string
	StreamTime      time.Time
	SignalWriteTime time.Time
	Asset           Asset
	Side            OrderSide
	Amount          float64
	SignalType      SignalType
	Allocations     map[Asset]float64
}

func (s *Signal) GetId() string {
	return s.SignalId
}

func (s *Signal) GetTimestamp() time.Time {
	return s.SignalWriteTime
}

type RewardRecord struct {
	WriteTime       time.Time
	SignalId        string
	StreamTime      time.Time
	Reward          float64
}

func (rr *RewardRecord) GetId() string {
	return rr.SignalId
}

func (rr *RewardRecord) GetTimestamp() time.Time {
	return rr.StreamTime
}
