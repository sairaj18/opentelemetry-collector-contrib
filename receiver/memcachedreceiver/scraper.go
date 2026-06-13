// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package memcachedreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/memcachedreceiver"

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/memcachedreceiver/internal/metadata"
)

type memcachedScraper struct {
	logger    *zap.Logger
	config    *Config
	mb        *metadata.MetricsBuilder
	newClient newMemcachedClientFunc
}

func newMemcachedScraper(
	settings receiver.Settings,
	config *Config,
) memcachedScraper {
	return memcachedScraper{
		logger:    settings.Logger,
		config:    config,
		newClient: newMemcachedClient,
		mb:        metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
	}
}

func (r *memcachedScraper) scrape(_ context.Context) (pmetric.Metrics, error) {
	// Init client in scrape method in case there are transient errors in the
	// constructor.
	statsClient, err := r.newClient(r.config.Endpoint, r.config.Timeout)
	if err != nil {
		r.logger.Error("Failed to establish client", zap.Error(err))
		return pmetric.Metrics{}, err
	}

	allServerStats, err := statsClient.Stats()
	if err != nil {
		r.logger.Error("Failed to fetch memcached stats", zap.Error(err))
		return pmetric.Metrics{}, err
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	for _, stats := range allServerStats {
		for k, v := range stats.Stats {
			switch k {
			case "bytes":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedBytesDataPoint(now, parsedV)
				}
			case "curr_connections":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedConnectionsCurrentDataPoint(now, parsedV)
				}
			case "total_connections":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedConnectionsTotalDataPoint(now, parsedV)
				}
			case "cmd_get":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedCommandsDataPoint(now, parsedV, metadata.AttributeCommandGet)
				}
			case "cmd_set":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedCommandsDataPoint(now, parsedV, metadata.AttributeCommandSet)
				}
			case "cmd_flush":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedCommandsDataPoint(now, parsedV, metadata.AttributeCommandFlush)
				}
			case "cmd_touch":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedCommandsDataPoint(now, parsedV, metadata.AttributeCommandTouch)
				}
			case "curr_items":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedCurrentItemsDataPoint(now, parsedV)
				}

			case "threads":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedThreadsDataPoint(now, parsedV)
				}

			case "evictions":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedEvictionsDataPoint(now, parsedV)
				}
			case "bytes_read":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedNetworkDataPoint(now, parsedV, metadata.AttributeDirectionReceived)
				}
			case "bytes_written":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedNetworkDataPoint(now, parsedV, metadata.AttributeDirectionSent)
				}
			case "get_hits":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeHit,
						metadata.AttributeOperationGet)
				}
			case "get_misses":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeMiss,
						metadata.AttributeOperationGet)
				}
			case "incr_hits":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeHit,
						metadata.AttributeOperationIncrement)
				}
			case "incr_misses":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeMiss,
						metadata.AttributeOperationIncrement)
				}
			case "decr_hits":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeHit,
						metadata.AttributeOperationDecrement)
				}
			case "decr_misses":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedOperationsDataPoint(now, parsedV, metadata.AttributeTypeMiss,
						metadata.AttributeOperationDecrement)
				}
			case "rusage_system":
				if parsedV, ok := r.parseFloat(k, v); ok {
					r.mb.RecordMemcachedCPUUsageDataPoint(now, parsedV, metadata.AttributeStateSystem)
				}

			case "rusage_user":
				if parsedV, ok := r.parseFloat(k, v); ok {
					r.mb.RecordMemcachedCPUUsageDataPoint(now, parsedV, metadata.AttributeStateUser)
				}
			// Global slab stats (from "stats slabs"; parsed into the top-level Stats map).
			case "active_slabs":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedSlabsCountDataPoint(now, parsedV)
				}
			case "total_malloced":
				if parsedV, ok := r.parseInt(k, v); ok {
					r.mb.RecordMemcachedSlabsAllocatedMemoryDataPoint(now, parsedV)
				}
			}
		}

		// Per-slab-class stats from the "stats slabs" command.
		for slabID, slabStats := range stats.Slabs {
			id := int64(slabID)
			for k, v := range slabStats {
				switch k {
				case "chunk_size":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsChunkSizeDataPoint(now, parsedV, id)
					}
				case "chunks_per_page":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsChunksPerPageDataPoint(now, parsedV, id)
					}
				case "total_pages":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsPagesDataPoint(now, parsedV, id)
					}
				case "total_chunks":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsChunksDataPoint(now, parsedV, id, metadata.AttributeSlabChunkStateTotal)
					}
				case "used_chunks":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsChunksDataPoint(now, parsedV, id, metadata.AttributeSlabChunkStateUsed)
					}
				case "free_chunks":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsChunksDataPoint(now, parsedV, id, metadata.AttributeSlabChunkStateFree)
					}
				case "mem_requested":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsRequestedMemoryDataPoint(now, parsedV, id)
					}
				case "cmd_set":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationSet)
					}
				case "get_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationGet)
					}
				case "delete_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationDelete)
					}
				case "incr_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationIncrement)
					}
				case "decr_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationDecrement)
					}
				case "touch_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationTouch)
					}
				case "cas_hits":
					if parsedV, ok := r.parseInt(k, v); ok {
						r.mb.RecordMemcachedSlabsOperationsDataPoint(now, parsedV, id, metadata.AttributeSlabOperationCas)
					}
				}
			}
		}

		// Calculated Metrics
		parsedHit, okHit := r.parseInt("incr_hits", stats.Stats["incr_hits"])
		parsedMiss, okMiss := r.parseInt("incr_misses", stats.Stats["incr_misses"])
		if okHit && okMiss {
			r.mb.RecordMemcachedOperationHitRatioDataPoint(now, calculateHitRatio(parsedHit, parsedMiss),
				metadata.AttributeOperationIncrement)
		}

		parsedHit, okHit = r.parseInt("decr_hits", stats.Stats["decr_hits"])
		parsedMiss, okMiss = r.parseInt("decr_misses", stats.Stats["decr_misses"])
		if okHit && okMiss {
			r.mb.RecordMemcachedOperationHitRatioDataPoint(now, calculateHitRatio(parsedHit, parsedMiss),
				metadata.AttributeOperationDecrement)
		}

		parsedHit, okHit = r.parseInt("get_hits", stats.Stats["get_hits"])
		parsedMiss, okMiss = r.parseInt("get_misses", stats.Stats["get_misses"])
		if okHit && okMiss {
			r.mb.RecordMemcachedOperationHitRatioDataPoint(now, calculateHitRatio(parsedHit, parsedMiss), metadata.AttributeOperationGet)
		}
	}

	return r.mb.Emit(), nil
}

func calculateHitRatio(misses, hits int64) float64 {
	if misses+hits == 0 {
		return 0
	}
	hitsFloat := float64(hits)
	missesFloat := float64(misses)
	return hitsFloat / (hitsFloat + missesFloat) * 100
}

// parseInt converts string to int64.
func (r *memcachedScraper) parseInt(key, value string) (int64, bool) {
	i, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		r.logInvalid("int", key, value)
		return 0, false
	}
	return i, true
}

// parseFloat converts string to float64.
func (r *memcachedScraper) parseFloat(key, value string) (float64, bool) {
	i, err := strconv.ParseFloat(value, 64)
	if err != nil {
		r.logInvalid("float", key, value)
		return 0, false
	}
	return i, true
}

func (r *memcachedScraper) logInvalid(expectedType, key, value string) {
	r.logger.Info(
		"invalid value",
		zap.String("expectedType", expectedType),
		zap.String("key", key),
		zap.String("value", value),
	)
}
