<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Prometheus\Storage;

use InvalidArgumentException;
use MakiseCo\Redis\RedisLazyConnection;
use MakiseCo\Redis\RedisPool;
use Predis\Client;
use Prometheus\Counter;
use Prometheus\Exception\StorageException;
use Prometheus\Gauge;
use Prometheus\Histogram;
use Prometheus\MetricFamilySamples;
use Prometheus\Storage\Adapter;
use Redis;

use function array_keys;
use function array_map;
use function array_merge;
use function array_unique;
use function implode;
use function json_decode;
use function json_encode;
use function sort;
use function strcmp;
use function usort;

/**
 * @author Dmitry K. <coder1994@gmail.com> - adopting for Swoole
 * @author Daniel Noel-Davies <Daniel.Noel-Davies@endclothing.com> - original code
 */
class MakisePRedisStorage implements Adapter
{
    private const PROMETHEUS_METRIC_KEYS_SUFFIX = '_METRIC_KEYS';

    private string $prefix;

    private RedisPool $pool;

    public function __construct(RedisPool $pool, string $prefix)
    {
        $this->pool = $pool;
        $this->prefix = $prefix;
    }

    /**
     * @return MetricFamilySamples[]
     * @throws StorageException
     */
    public function collect(): array
    {
        $redis = $this->openConnection();
        $metrics = $this->collectHistograms($redis);
        $metrics = array_merge($metrics, $this->collectGauges($redis));
        $metrics = array_merge($metrics, $this->collectCounters($redis));
        return array_map(
            function (array $metric) {
                return new MetricFamilySamples($metric);
            },
            $metrics
        );
    }

    private function openConnection(): RedisLazyConnection
    {
        return new RedisLazyConnection($this->pool);
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateHistogram(array $data): void
    {
        $redis = $this->openConnection();
        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }
        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['labelValues']);

        $client = new Client();
        $redis->eval(
            <<<LUA
local increment = redis.call('hIncrByFloat', KEYS[1], KEYS[2], ARGV[1])
redis.call('hIncrBy', KEYS[1], KEYS[3], 1)
if increment == ARGV[1] then
    redis.call('hSet', KEYS[1], '__meta', ARGV[2])
    redis.call('sAdd', KEYS[4], KEYS[1])
end
LUA
            ,
            4,
            $this->toMetricKey($data),
            json_encode(['b' => 'sum', 'labelValues' => $data['labelValues']]),
            json_encode(['b' => $bucketToIncrease, 'labelValues' => $data['labelValues']]),
            $this->prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            $data['value'],
            json_encode($metaData),
        );
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateGauge(array $data): void
    {
        $redis = $this->openConnection();

        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['labelValues']);
        unset($metaData['command']);
        $redis->eval(
            <<<LUA
local result = redis.call(KEYS[2], KEYS[1], KEYS[4], ARGV[1])

if KEYS[2] == 'hSet' then
    if result == 1 then
        redis.call('hSet', KEYS[1], '__meta', ARGV[2])
        redis.call('sAdd', KEYS[3], KEYS[1])
    end
else
    if result == ARGV[1] then
        redis.call('hSet', KEYS[1], '__meta', ARGV[2])
        redis.call('sAdd', KEYS[3], KEYS[1])
    end
end
LUA
            ,
            4,
            $this->toMetricKey($data),
            $this->getRedisCommand($data['command']),
            $this->prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            json_encode($data['labelValues']),
            $data['value'],
            json_encode($metaData),
        );
    }

    /**
     * @param array $data
     * @throws StorageException
     */
    public function updateCounter(array $data): void
    {
        $redis = $this->openConnection();
        $metaData = $data;
        unset($metaData['value']);
        unset($metaData['labelValues']);
        unset($metaData['command']);
        $redis->eval(
            <<<LUA
local result = redis.call(KEYS[2], KEYS[1], KEYS[4], ARGV[1])
if result == tonumber(ARGV[1]) then
    redis.call('hMSet', KEYS[1], '__meta', ARGV[2])
    redis.call('sAdd', KEYS[3], KEYS[1])
end
return result
LUA
            ,
            4,
            $this->toMetricKey($data),
            $this->getRedisCommand($data['command']),
            $this->prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            json_encode($data['labelValues']),
            $data['value'],
            json_encode($metaData),
        );
    }

    /**
     * @param Redis $redis
     * @return array
     */
    private function collectHistograms(Redis $redis): array
    {
        $keys = $redis->sMembers($this->prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $histograms = [];
        foreach ($keys as $key) {
            $raw = $redis->hGetAll($key);
            $histogram = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $histogram['samples'] = [];

            // Add the Inf bucket so we can compute it later on
            $histogram['buckets'][] = '+Inf';

            $allLabelValues = [];
            foreach (array_keys($raw) as $k) {
                $d = json_decode($k, true);
                if ($d['b'] == 'sum') {
                    continue;
                }
                $allLabelValues[] = $d['labelValues'];
            }

            // We need set semantics.
            // This is the equivalent of array_unique but for arrays of arrays.
            $allLabelValues = array_map("unserialize", array_unique(array_map("serialize", $allLabelValues)));
            sort($allLabelValues);

            foreach ($allLabelValues as $labelValues) {
                // Fill up all buckets.
                // If the bucket doesn't exist fill in values from
                // the previous one.
                $acc = 0;
                foreach ($histogram['buckets'] as $bucket) {
                    $bucketKey = json_encode(['b' => $bucket, 'labelValues' => $labelValues]);
                    if (!isset($raw[$bucketKey])) {
                        $histogram['samples'][] = [
                            'name' => $histogram['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($labelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    } else {
                        $acc += $raw[$bucketKey];
                        $histogram['samples'][] = [
                            'name' => $histogram['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge($labelValues, [$bucket]),
                            'value' => $acc,
                        ];
                    }
                }

                // Add the count
                $histogram['samples'][] = [
                    'name' => $histogram['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $labelValues,
                    'value' => $acc,
                ];

                // Add the sum
                $histogram['samples'][] = [
                    'name' => $histogram['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $labelValues,
                    'value' => $raw[json_encode(['b' => 'sum', 'labelValues' => $labelValues])],
                ];
            }
            $histograms[] = $histogram;
        }
        return $histograms;
    }

    /**
     * @param Redis $redis
     * @return array
     */
    private function collectGauges(Redis $redis): array
    {
        $keys = $redis->sMembers($this->prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $gauges = [];
        foreach ($keys as $key) {
            $raw = $redis->hGetAll($key);
            $gauge = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $gauge['samples'] = [];
            foreach ($raw as $k => $value) {
                $gauge['samples'][] = [
                    'name' => $gauge['name'],
                    'labelNames' => [],
                    'labelValues' => json_decode($k, true),
                    'value' => $value,
                ];
            }
            usort(
                $gauge['samples'],
                function ($a, $b) {
                    return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
                }
            );
            $gauges[] = $gauge;
        }
        return $gauges;
    }

    /**
     * @param Redis $redis
     * @return array
     */
    private function collectCounters(Redis $redis): array
    {
        $keys = $redis->sMembers($this->prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $counters = [];
        foreach ($keys as $key) {
            $raw = $redis->hGetAll($key);
            $counter = json_decode($raw['__meta'], true);
            unset($raw['__meta']);
            $counter['samples'] = [];
            foreach ($raw as $k => $value) {
                $counter['samples'][] = [
                    'name' => $counter['name'],
                    'labelNames' => [],
                    'labelValues' => json_decode($k, true),
                    'value' => $value,
                ];
            }
            usort(
                $counter['samples'],
                function ($a, $b) {
                    return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
                }
            );
            $counters[] = $counter;
        }
        return $counters;
    }

    /**
     * @param int $cmd
     * @return string
     */
    private function getRedisCommand(int $cmd): string
    {
        switch ($cmd) {
            case Adapter::COMMAND_INCREMENT_INTEGER:
                return 'hIncrBy';
            case Adapter::COMMAND_INCREMENT_FLOAT:
                return 'hIncrByFloat';
            case Adapter::COMMAND_SET:
                return 'hSet';
            default:
                throw new InvalidArgumentException("Unknown command");
        }
    }

    /**
     * @param array $data
     * @return string
     */
    private function toMetricKey(array $data): string
    {
        return implode(':', [$this->prefix, $data['type'], $data['name']]);
    }
}
