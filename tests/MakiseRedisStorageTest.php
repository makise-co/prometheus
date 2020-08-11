<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Prometheus\Tests;

use MakiseCo\Prometheus\Storage\MakiseRedisStorage;
use MakiseCo\Redis\RedisPool;
use PHPUnit\Framework\TestCase;
use Prometheus\CollectorRegistry;
use Prometheus\RenderTextFormat;

class MakiseRedisStorageTest extends TestCase
{
    use CoroutineTestTrait;

    public function testItWorks(): void
    {
        $this->runCoroWithPool(static function (RedisPool $pool) {
            $adapter = new MakiseRedisStorage($pool, 'test_makise');
            $collector = new CollectorRegistry($adapter);

            $gauge = $collector->getOrRegisterGauge('test', 'makise', '123');
            $gauge->set(24);

            $renderer = new RenderTextFormat();
            $content = $renderer->render($collector->getMetricFamilySamples());

            self::assertNotFalse(\mb_strpos($content, 'test_makise 24'));
        });
    }
}
