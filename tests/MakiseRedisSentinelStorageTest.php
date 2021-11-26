<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Prometheus\Tests;

use MakiseCo\Prometheus\Storage\MakiseRedisSentinelStorage;
use MakiseCo\Prometheus\Storage\MakiseRedisStorage;
use MakiseCo\Redis\RedisPool;
use PHPUnit\Framework\TestCase;
use Prometheus\CollectorRegistry;
use Prometheus\RenderTextFormat;

class MakiseRedisSentinelStorageTest extends TestCase
{
    use CoroutineTestTrait;

    public function testItWorks(): void
    {
        $this->runCoroWithSentinelPool(static function (RedisPool $pool) {
            $adapter = new MakiseRedisSentinelStorage($pool, 'test_makise2');
            $collector = new CollectorRegistry($adapter);

            $adapter->wipeStorage();

            $gauge = $collector->getOrRegisterGauge('test', 'makise', '123');
            $gauge->set(24);

            $counter = $collector->getOrRegisterCounter('test', 'makise', '123');
            $counter->incBy(100);

            $renderer = new RenderTextFormat();
            $content = $renderer->render($collector->getMetricFamilySamples());

            self::assertNotFalse(\mb_strpos($content, 'test_makise 24'));
            self::assertNotFalse(\mb_strpos($content, 'test_makise 100'));
        });
    }
}
