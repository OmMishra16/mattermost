// Copyright (c) 2015-present Mattermost, Inc. All Rights Reserved.
// See LICENSE.txt for license information.

package model

import (
	"net/http"
)

// RedisSettings stores the configuration for Redis connection
type RedisSettings struct {
	Enable               *bool   `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	ClusterAddress       *string `access:"environment_high_availability,write_restrictable,cloud_restrictable"` // telemetry: none
	Password             *string `access:"environment_high_availability,write_restrictable,cloud_restrictable"` // telemetry: none
	Database             *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"` // telemetry: none
	MaxIdleConns         *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	MaxActiveConns       *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	IdleTimeoutSecs      *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	ConnectTimeoutSecs   *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	ReadTimeoutSecs      *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
	WriteTimeoutSecs     *int    `access:"environment_high_availability,write_restrictable,cloud_restrictable"`
}

// SetDefaults sets the default values for Redis settings
func (s *RedisSettings) SetDefaults() {
	if s.Enable == nil {
		s.Enable = NewPointer(false)
	}

	if s.ClusterAddress == nil {
		s.ClusterAddress = NewPointer("localhost:6379")
	}

	if s.Password == nil {
		s.Password = NewPointer("")
	}

	if s.Database == nil {
		s.Database = NewPointer(0)
	}

	if s.MaxIdleConns == nil {
		s.MaxIdleConns = NewPointer(10)
	}

	if s.MaxActiveConns == nil {
		s.MaxActiveConns = NewPointer(20)
	}

	if s.IdleTimeoutSecs == nil {
		s.IdleTimeoutSecs = NewPointer(300)
	}

	if s.ConnectTimeoutSecs == nil {
		s.ConnectTimeoutSecs = NewPointer(5)
	}

	if s.ReadTimeoutSecs == nil {
		s.ReadTimeoutSecs = NewPointer(3)
	}

	if s.WriteTimeoutSecs == nil {
		s.WriteTimeoutSecs = NewPointer(3)
	}
}

// IsValid validates the Redis settings
func (s *RedisSettings) IsValid() *AppError {
	if *s.Enable && *s.ClusterAddress == "" {
		return NewAppError("Config.IsValid", "model.config.is_valid.empty_redis_cluster_address.app_error", nil, "", http.StatusBadRequest)
	}

	if *s.Database < 0 {
		return NewAppError("Config.IsValid", "model.config.is_valid.invalid_redis_db.app_error", nil, "", http.StatusBadRequest)
	}

	return nil
}
