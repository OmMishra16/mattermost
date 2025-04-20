// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package platform

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

const (
	RedisChannelWebSocketEvents = "mattermost:cluster:websocket_events"
	RedisChannelUserPresence    = "mattermost:cluster:user_presence"
	RedisChannelTypingEvents    = "mattermost:cluster:typing_events"
	RedisKeyClusterNodes        = "mattermost:cluster:nodes"
	RedisKeepAliveInterval      = 30 * time.Second
)

// RedisCluster manages Redis-based cluster communication
type RedisCluster struct {
	ps              *PlatformService
	client          *redis.Client
	pubsub          *redis.PubSub
	ctx             context.Context
	cancel          context.CancelFunc
	nodeID          string
	isActive        bool
	lock            sync.RWMutex
	redisEventsChan <-chan *redis.Message
}

// NewRedisCluster creates a new RedisCluster
func NewRedisCluster(ps *PlatformService) *RedisCluster {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &RedisCluster{
		ps:        ps,
		ctx:       ctx,
		cancel:    cancel,
		nodeID:    model.NewId(),
		isActive:  false,
	}
}

// Start initializes Redis connections and subscriptions
func (rc *RedisCluster) Start() error {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	
	if rc.isActive {
		return nil
	}
	
	// Initialize Redis client
	rc.client = redis.NewClient(&redis.Options{
		Addr:     rc.ps.Config().RedisSettings.ClusterAddress,
		Password: rc.ps.Config().RedisSettings.Password,
		DB:       rc.ps.Config().RedisSettings.Database,
	})
	
	// Test connection
	if _, err := rc.client.Ping(rc.ctx).Result(); err != nil {
		mlog.Error("Failed to connect to Redis for clustering", mlog.Err(err))
		return err
	}
	
	// Subscribe to cluster channels
	rc.pubsub = rc.client.Subscribe(rc.ctx,
		RedisChannelWebSocketEvents,
		RedisChannelUserPresence,
		RedisChannelTypingEvents,
	)
	
	rc.redisEventsChan = rc.pubsub.Channel()
	
	// Register node in cluster
	nodeInfo := map[string]interface{}{
		"id":        rc.nodeID,
		"timestamp": time.Now().UnixNano(),
	}
	
	nodeInfoJSON, _ := json.Marshal(nodeInfo)
	rc.client.HSet(rc.ctx, RedisKeyClusterNodes, rc.nodeID, nodeInfoJSON)
	
	// Set expiration for node entry
	rc.client.Expire(rc.ctx, RedisKeyClusterNodes, 2*RedisKeepAliveInterval)
	
	// Start processing Redis events
	go rc.processRedisEvents()
	
	// Start keep-alive routine
	go rc.keepAlive()
	
	rc.isActive = true
	
	mlog.Info("Redis cluster initialized", mlog.String("node_id", rc.nodeID))
	
	return nil
}

// Stop terminates Redis connections and subscriptions
func (rc *RedisCluster) Stop() {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	
	if !rc.isActive {
		return
	}
	
	// Cancel context to stop all goroutines
	rc.cancel()
	
	// Remove this node from cluster
	rc.client.HDel(context.Background(), RedisKeyClusterNodes, rc.nodeID)
	
	// Close connections
	if rc.pubsub != nil {
		rc.pubsub.Close()
	}
	
	if rc.client != nil {
		rc.client.Close()
	}
	
	rc.isActive = false
	
	mlog.Info("Redis cluster connections closed", mlog.String("node_id", rc.nodeID))
}

// GetNodeID returns this node's unique ID
func (rc *RedisCluster) GetNodeID() string {
	return rc.nodeID
}

// PublishWebSocketEvent publishes a WebSocket event to all cluster nodes
func (rc *RedisCluster) PublishWebSocketEvent(event *model.WebSocketEvent) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	
	if !rc.isActive {
		return
	}
	
	// Add origin node ID to the event
	event.SetBroadcastData("origin_node_id", rc.nodeID)
	
	// Serialize the event
	jsonEvent, err := json.Marshal(event)
	if err != nil {
		mlog.Error("Failed to serialize WebSocket event", mlog.Err(err))
		return
	}
	
	// Publish to Redis
	if err := rc.client.Publish(rc.ctx, RedisChannelWebSocketEvents, jsonEvent).Err(); err != nil {
		mlog.Error("Failed to publish WebSocket event to Redis", mlog.Err(err))
	}
}

// PublishUserPresence publishes a user presence update to all cluster nodes
func (rc *RedisCluster) PublishUserPresence(userID string, status string, manual bool, activityAt int64) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	
	if !rc.isActive {
		return
	}
	
	presenceEvent := map[string]interface{}{
		"user_id":      userID,
		"status":       status,
		"manual":       manual,
		"activity_at":  activityAt,
		"node_id":      rc.nodeID,
	}
	
	// Serialize the event
	jsonEvent, err := json.Marshal(presenceEvent)
	if err != nil {
		mlog.Error("Failed to serialize presence event", mlog.Err(err))
		return
	}
	
	// Publish to Redis
	if err := rc.client.Publish(rc.ctx, RedisChannelUserPresence, jsonEvent).Err(); err != nil {
		mlog.Error("Failed to publish presence event to Redis", mlog.Err(err))
	}
}

// PublishTypingEvent publishes typing status to all cluster nodes
func (rc *RedisCluster) PublishTypingEvent(userID, channelID, parentID string) {
	rc.lock.RLock()
	defer rc.lock.RUnlock()
	
	if !rc.isActive {
		return
	}
	
	typingEvent := map[string]interface{}{
		"user_id":     userID,
		"channel_id":  channelID,
		"parent_id":   parentID,
		"node_id":     rc.nodeID,
	}
	
	// Serialize the event
	jsonEvent, err := json.Marshal(typingEvent)
	if err != nil {
		mlog.Error("Failed to serialize typing event", mlog.Err(err))
		return
	}
	
	// Publish to Redis
	if err := rc.client.Publish(rc.ctx, RedisChannelTypingEvents, jsonEvent).Err(); err != nil {
		mlog.Error("Failed to publish typing event to Redis", mlog.Err(err))
	}
}

// processRedisEvents handles messages from Redis subscriptions
func (rc *RedisCluster) processRedisEvents() {
	for {
		select {
		case <-rc.ctx.Done():
			return
			
		case msg, ok := <-rc.redisEventsChan:
			if !ok {
				mlog.Error("Redis subscription channel closed")
				return
			}
			
			switch msg.Channel {
			case RedisChannelWebSocketEvents:
				rc.handleWebSocketEvent(msg.Payload)
				
			case RedisChannelUserPresence:
				rc.handlePresenceEvent(msg.Payload)
				
			case RedisChannelTypingEvents:
				rc.handleTypingEvent(msg.Payload)
			}
		}
	}
}

// handleWebSocketEvent processes WebSocket events from other nodes
func (rc *RedisCluster) handleWebSocketEvent(payload string) {
	var event model.WebSocketEvent
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		mlog.Error("Failed to unmarshal WebSocket event", mlog.Err(err))
		return
	}
	
	// Skip if this event originated from this node
	if originNodeID, ok := event.GetBroadcastData()["origin_node_id"].(string); ok && originNodeID == rc.nodeID {
		return
	}
	
	// Remove the origin node ID to prevent loops if this event gets republished
	event.RemoveBroadcastData("origin_node_id")
	
	// Forward to local WebSocket hub without republishing to Redis
	rc.ps.PublishSkipClusterSend(&event)
}

// handlePresenceEvent processes presence events from other nodes
func (rc *RedisCluster) handlePresenceEvent(payload string) {
	var presenceEvent map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &presenceEvent); err != nil {
		mlog.Error("Failed to unmarshal presence event", mlog.Err(err))
		return
	}
	
	// Skip if this event originated from this node
	if nodeID, ok := presenceEvent["node_id"].(string); ok && nodeID == rc.nodeID {
		return
	}
	
	// Extract event data
	userID, _ := presenceEvent["user_id"].(string)
	status, _ := presenceEvent["status"].(string)
	activityAtFloat, _ := presenceEvent["activity_at"].(float64)
	activityAt := int64(activityAtFloat)
	manual, _ := presenceEvent["manual"].(bool)
	
	// Update local status cache
	if userID != "" && status != "" {
		rc.ps.AddStatusCache(&model.Status{
			UserId:         userID,
			Status:         status,
			Manual:         manual,
			LastActivityAt: activityAt,
		})
		
		// Broadcast to local WebSocket connections
		statusEvent := model.NewWebSocketEvent(model.WebsocketEventStatusChange, "", "", "", nil, "")
		statusEvent.Add("user_id", userID)
		statusEvent.Add("status", status)
		rc.ps.PublishSkipClusterSend(statusEvent)
	}
}

// handleTypingEvent processes typing events from other nodes
func (rc *RedisCluster) handleTypingEvent(payload string) {
	var typingEvent map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &typingEvent); err != nil {
		mlog.Error("Failed to unmarshal typing event", mlog.Err(err))
		return
	}
	
	// Skip if this event originated from this node
	if nodeID, ok := typingEvent["node_id"].(string); ok && nodeID == rc.nodeID {
		return
	}
	
	// Extract event data
	userID, _ := typingEvent["user_id"].(string)
	channelID, _ := typingEvent["channel_id"].(string)
	parentID, _ := typingEvent["parent_id"].(string)
	
	// Create typing WebSocket event
	typingWSEvent := model.NewWebSocketEvent(model.WebsocketEventTyping, channelID, "", userID, nil, "")
	if parentID != "" {
		typingWSEvent.Add("parent_id", parentID)
	}
	
	// Broadcast to local WebSocket connections
	rc.ps.PublishSkipClusterSend(typingWSEvent)
}

// keepAlive periodically updates this node's entry in Redis
func (rc *RedisCluster) keepAlive() {
	ticker := time.NewTicker(RedisKeepAliveInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-rc.ctx.Done():
			return
			
		case <-ticker.C:
			rc.lock.RLock()
			if !rc.isActive {
				rc.lock.RUnlock()
				return
			}
			
			// Update node timestamp
			nodeInfo := map[string]interface{}{
				"id":        rc.nodeID,
				"timestamp": time.Now().UnixNano(),
			}
			
			nodeInfoJSON, _ := json.Marshal(nodeInfo)
			err := rc.client.HSet(rc.ctx, RedisKeyClusterNodes, rc.nodeID, nodeInfoJSON).Err()
			
			// Refresh expiration
			rc.client.Expire(rc.ctx, RedisKeyClusterNodes, 2*RedisKeepAliveInterval)
			
			rc.lock.RUnlock()
			
			if err != nil {
				mlog.Error("Failed to update node keepalive", mlog.Err(err))
			}
		}
	}
}

// GetClusterNodes returns a list of active nodes in the cluster
func (rc *RedisCluster) GetClusterNodes() []string {
	if !rc.isActive {
		return []string{rc.nodeID}
	}
	
	nodes, err := rc.client.HGetAll(rc.ctx, RedisKeyClusterNodes).Result()
	if err != nil {
		mlog.Error("Failed to get cluster nodes", mlog.Err(err))
		return []string{rc.nodeID}
	}
	
	nodeIDs := make([]string, 0, len(nodes))
	for id := range nodes {
		nodeIDs = append(nodeIDs, id)
	}
	
	return nodeIDs
}
