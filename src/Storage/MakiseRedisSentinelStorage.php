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
use MakiseCo\Redis\RedisPool;
use Prometheus\Counter;
use Prometheus\Exception\StorageException;
use Prometheus\Gauge;
use Prometheus\Histogram;
use Prometheus\Math;
use Prometheus\MetricFamilySamples;
use Prometheus\Storage\Adapter;
use Prometheus\Summary;
use RuntimeException;

class MakiseRedisSentinelStorage implements Adapter
{
    public const PROMETHEUS_METRIC_KEYS_SUFFIX = '_METRIC_KEYS';

    private string $prefix;

    private RedisPool $redis;

    public function __construct(RedisPool $pool, string $prefix)
    {
        $this->redis = $pool;
        $this->prefix = $prefix;
    }

    /**
     * @param string $prefix
     */
    public function setPrefix(string $prefix): void
    {
        $this->prefix = $prefix;
    }

    /**
     * @inheritDoc
     */
    public function wipeStorage(): void
    {
        $searchPattern = "";

        $globalPrefix = (string)$this->redis->getOptions()->prefix;
        // @phpstan-ignore-next-line false positive, phpstan thinks getOptions returns int
        if (is_string($globalPrefix)) {
            $searchPattern .= $globalPrefix;
        }

        $searchPattern .= $this->prefix;
        $searchPattern .= '*';

        $this->redis->eval(
            <<<LUA
local cursor = "0"
repeat
    local results = redis.call('SCAN', cursor, 'MATCH', ARGV[1])
    cursor = results[1]
    for _, key in ipairs(results[2]) do
        redis.call('DEL', key)
    end
until cursor == "0"
LUA
            ,
            0,
            $searchPattern,
        );
    }

    /**
     * @param mixed[] $data
     *
     * @return string
     */
    private function metaKey(array $data): string
    {
        return implode(':', [
            $data['name'],
            'meta'
        ]);
    }

    /**
     * @param mixed[] $data
     *
     * @return string
     */
    private function valueKey(array $data): string
    {
        return implode(':', [
            $data['name'],
            $this->encodeLabelValues($data['labelValues']),
            'value'
        ]);
    }

    /**
     * @return MetricFamilySamples[]
     * @throws StorageException
     */
    public function collect(): array
    {
        $metrics = $this->collectHistograms();
        $metrics = array_merge($metrics, $this->collectGauges());
        $metrics = array_merge($metrics, $this->collectCounters());
        $metrics = array_merge($metrics, $this->collectSummaries());

        return array_map(
            static function (array $metric): MetricFamilySamples {
                return new MetricFamilySamples($metric);
            },
            $metrics
        );
    }

    /**
     * @param mixed[] $data
     * @throws StorageException
     */
    public function updateHistogram(array $data): void
    {
        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }
        $metaData = $data;
        unset($metaData['value'], $metaData['labelValues']);

        $this->redis->eval(
            <<<LUA
local result = redis.call('hIncrByFloat', KEYS[1], ARGV[1], ARGV[3])
redis.call('hIncrBy', KEYS[1], ARGV[2], 1)
if tonumber(result) >= tonumber(ARGV[3]) then
    redis.call('hSet', KEYS[1], '__meta', ARGV[4])
    redis.call('sAdd', KEYS[2], KEYS[1])
end
return result
LUA
            ,
            2,
            $this->toMetricKey($data),
            $this->prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            json_encode(['b' => 'sum', 'labelValues' => $data['labelValues']]),
            json_encode(['b' => $bucketToIncrease, 'labelValues' => $data['labelValues']]),
            $data['value'],
            json_encode($metaData),
        );
    }

    /**
     * @param mixed[] $data
     * @throws StorageException
     */
    public function updateSummary(array $data): void
    {
        // store meta
        $summaryKey = $this->prefix . Summary::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX;
        $metaKey = $summaryKey . ':' . $this->metaKey($data);
        $json = json_encode($this->metaData($data));
        if (false === $json) {
            throw new RuntimeException(json_last_error_msg());
        }
        $this->redis->setNx($metaKey, $json);

        // store value key
        $valueKey = $summaryKey . ':' . $this->valueKey($data);
        $json = json_encode($this->encodeLabelValues($data['labelValues']));
        if (false === $json) {
            throw new RuntimeException(json_last_error_msg());
        }
        $this->redis->setNx($valueKey, $json);

        // trick to handle uniqid collision
        $done = false;
        while (!$done) {
            $sampleKey = $valueKey . ':' . uniqid('', true);
            $done = $this->redis->set($sampleKey, $data['value'], ['NX', 'EX' => $data['maxAgeSeconds']]);
        }
    }

    /**
     * @param mixed[] $data
     * @throws StorageException
     */
    public function updateGauge(array $data): void
    {
        $metaData = $data;
        unset($metaData['value'], $metaData['labelValues'], $metaData['command']);
        $this->redis->eval(
            <<<LUA
local result = redis.call(ARGV[1], KEYS[1], ARGV[2], ARGV[3])

if ARGV[1] == 'hSet' then
    if result == 1 then
        redis.call('hSet', KEYS[1], '__meta', ARGV[4])
        redis.call('sAdd', KEYS[2], KEYS[1])
    end
else
    if result == ARGV[3] then
        redis.call('hSet', KEYS[1], '__meta', ARGV[4])
        redis.call('sAdd', KEYS[2], KEYS[1])
    end
end
LUA
            ,
            2,
            $this->toMetricKey($data),
            $this->prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            $this->getRedisCommand($data['command']),
            json_encode($data['labelValues']),
            $data['value'],
            json_encode($metaData),
        );
    }

    /**
     * @param mixed[] $data
     * @throws StorageException
     */
    public function updateCounter(array $data): void
    {
        $metaData = $data;
        unset($metaData['value'], $metaData['labelValues'], $metaData['command']);
        $this->redis->eval(
            <<<LUA
local result = redis.call(ARGV[1], KEYS[1], ARGV[3], ARGV[2])
local added = redis.call('sAdd', KEYS[2], KEYS[1])
if added == 1 then
    redis.call('hMSet', KEYS[1], '__meta', ARGV[4])
end
return result
LUA
            ,
            2,
            $this->toMetricKey($data),
            $this->prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX,
            $this->getRedisCommand($data['command']),
            $data['value'],
            json_encode($data['labelValues']),
            json_encode($metaData),
        );
    }


    /**
     * @param mixed[] $data
     * @return mixed[]
     */
    private function metaData(array $data): array
    {
        $metricsMetaData = $data;
        unset($metricsMetaData['value'], $metricsMetaData['command'], $metricsMetaData['labelValues']);
        return $metricsMetaData;
    }

    /**
     * @return mixed[]
     */
    private function collectHistograms(): array
    {
        $keys = $this->redis->sMembers($this->prefix . Histogram::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $histograms = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll($key);
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
     * @param string $key
     *
     * @return string
     */
    private function removePrefixFromKey(string $key): string
    {
        $redisPrefix = (string)$this->redis->getOptions()->prefix;

        // @phpstan-ignore-next-line false positive, phpstan thinks getOptions returns int
        if ($redisPrefix === '') {
            return $key;
        }
        // @phpstan-ignore-next-line false positive, phpstan thinks getOptions returns int
        return substr($key, strlen($redisPrefix));
    }

    /**
     * @return mixed[]
     */
    private function collectSummaries(): array
    {
        $math = new Math();
        $summaryKey = $this->prefix . Summary::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX;
        $keys = $this->redis->keys($summaryKey . ':*:meta');

        $summaries = [];
        foreach ($keys as $metaKeyWithPrefix) {
            $metaKey = $this->removePrefixFromKey($metaKeyWithPrefix);
            $rawSummary = $this->redis->get($metaKey);
            if ($rawSummary === false) {
                continue;
            }
            $summary = json_decode($rawSummary, true);
            $metaData = $summary;
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'maxAgeSeconds' => $metaData['maxAgeSeconds'],
                'quantiles' => $metaData['quantiles'],
                'samples' => [],
            ];

            $values = $this->redis->keys($summaryKey . ':' . $metaData['name'] . ':*:value');
            foreach ($values as $valueKeyWithPrefix) {
                $valueKey = $this->removePrefixFromKey($valueKeyWithPrefix);
                $rawValue = $this->redis->get($valueKey);
                if ($rawValue === false) {
                    continue;
                }
                $value = json_decode($rawValue, true);
                $encodedLabelValues = $value;
                $decodedLabelValues = $this->decodeLabelValues($encodedLabelValues);

                $samples = [];
                $sampleValues = $this->redis->keys($summaryKey . ':' . $metaData['name'] . ':' . $encodedLabelValues . ':value:*');
                foreach ($sampleValues as $sampleValueWithPrefix) {
                    $sampleValue = $this->removePrefixFromKey($sampleValueWithPrefix);
                    $samples[] = (float) $this->redis->get($sampleValue);
                }

                if (count($samples) === 0) {
                    $this->redis->del($valueKey);
                    continue;
                }

                // Compute quantiles
                sort($samples);
                foreach ($data['quantiles'] as $quantile) {
                    $data['samples'][] = [
                        'name' => $metaData['name'],
                        'labelNames' => ['quantile'],
                        'labelValues' => array_merge($decodedLabelValues, [$quantile]),
                        'value' => $math->quantile($samples, $quantile),
                    ];
                }

                // Add the count
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => count($samples),
                ];

                // Add the sum
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => array_sum($samples),
                ];
            }

            if (count($data['samples']) > 0) {
                $summaries[] = $data;
            } else {
                $this->redis->del($metaKey);
            }
        }
        return $summaries;
    }

    /**
     * @return mixed[]
     */
    private function collectGauges(): array
    {
        $keys = $this->redis->sMembers($this->prefix . Gauge::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $gauges = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll($key);
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
            usort($gauge['samples'], function ($a, $b): int {
                return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
            });
            $gauges[] = $gauge;
        }
        return $gauges;
    }

    /**
     * @return mixed[]
     */
    private function collectCounters(): array
    {
        $keys = $this->redis->sMembers($this->prefix . Counter::TYPE . self::PROMETHEUS_METRIC_KEYS_SUFFIX);
        sort($keys);
        $counters = [];
        foreach ($keys as $key) {
            $raw = $this->redis->hGetAll($key);
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
            usort($counter['samples'], function ($a, $b): int {
                return strcmp(implode("", $a['labelValues']), implode("", $b['labelValues']));
            });
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
     * @param mixed[] $data
     * @return string
     */
    private function toMetricKey(array $data): string
    {
        return implode(':', [$this->prefix, $data['type'], $data['name']]);
    }

    /**
     * @param mixed[] $values
     * @return string
     * @throws RuntimeException
     */
    private function encodeLabelValues(array $values): string
    {
        $json = json_encode($values);
        if (false === $json) {
            throw new RuntimeException(json_last_error_msg());
        }
        return base64_encode($json);
    }

    /**
     * @param string $values
     * @return mixed[]
     * @throws RuntimeException
     */
    private function decodeLabelValues(string $values): array
    {
        $json = base64_decode($values, true);
        if (false === $json) {
            throw new RuntimeException('Cannot base64 decode label values');
        }
        $decodedValues = json_decode($json, true);
        if (false === $decodedValues) {
            throw new RuntimeException(json_last_error_msg());
        }
        return $decodedValues;
    }
}
