package groups

import (
	"log"
	"sync"
	"time"

	"github.com/mkocikowski/libkafka/api/JoinGroup"
	"github.com/mkocikowski/libkafka/api/SyncGroup"
	"github.com/mkocikowski/libkafka/client"
	"github.com/mkocikowski/libkafka/errors"
)

type Assigner interface {
	Type() string
	Name() string
	Meta(string) []byte
	Assign([]JoinGroup.Member) ([]SyncGroup.Assignment, error)
}

type GroupMembershipManager struct {
	Bootstrap string
	Assigner  Assigner
	GroupId   string
	//
	sync.Mutex
	memberId     string
	generationId int32
	client       *client.GroupClient
	members      []JoinGroup.Member
	assignment   []byte
}

func (c *GroupMembershipManager) init() {
	c.client = &client.GroupClient{
		Bootstrap: c.Bootstrap,
		GroupId:   c.GroupId,
	}
}

func (c *GroupMembershipManager) join() error {
	c.Lock()
	defer c.Unlock()
	req := &client.JoinGroupRequest{
		MemberId:     c.memberId,
		ProtocolType: c.Assigner.Type(),
		ProtocolName: c.Assigner.Name(),
		Metadata:     c.Assigner.Meta(""),
	}
	resp, err := c.client.Join(req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != errors.NONE {
		return &errors.KafkaError{Code: resp.ErrorCode}
	}
	c.memberId = resp.MemberId
	c.generationId = resp.GenerationId
	c.members = resp.Members
	return nil
}

func (c *GroupMembershipManager) sync() error {
	c.Lock()
	defer c.Unlock()
	assignments, err := c.Assigner.Assign(c.members)
	if err != nil {
		return err
	}
	req := &client.SyncGroupRequest{
		MemberId:     c.memberId,
		GenerationId: c.generationId,
		Assignments:  assignments,
	}
	log.Printf("%+v", req)
	resp, err := c.client.Sync(req)
	if err != nil {
		return err
	}
	if resp.ErrorCode != errors.NONE {
		return &errors.KafkaError{Code: resp.ErrorCode}
	}
	c.assignment = resp.Assignment[:]
	return nil
}

func (c *GroupMembershipManager) heartbeat() error {
	c.Lock()
	defer c.Unlock()
	resp, err := c.client.Heartbeat(c.memberId, c.generationId)
	if err != nil {
		return err
	}
	if resp.ErrorCode != errors.NONE {
		return &errors.KafkaError{Code: resp.ErrorCode}
	}
	return nil
}

func (c *GroupMembershipManager) run(assignments chan<- []byte) {
	for {
		if err := c.heartbeat(); err == nil {
			time.Sleep(time.Second)
			continue
		} else {
			log.Println(err)
		}
		if err := c.join(); err != nil {
			log.Println(err)
			continue
		}
		if err := c.sync(); err != nil {
			log.Println(err)
			continue
		}
		assignments <- c.assignment
		time.Sleep(time.Second)
	}
}

func (c *GroupMembershipManager) Start() <-chan []byte {
	c.init()
	assignments := make(chan []byte)
	go c.run(assignments)
	return assignments
}
