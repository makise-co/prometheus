<?php
/*
 * This file is part of the Makise-Co Framework
 *
 * World line: 0.571024a
 * (c) Dmitry K. <coder1994@gmail.com>
 */

declare(strict_types=1);

namespace MakiseCo\Prometheus\Tests;

use Closure;
use MakiseCo\Redis\ConnectionConfig;
use MakiseCo\Redis\RedisPool;
use MakiseCo\Redis\RedisSentinelConnector;
use MakiseCo\Redis\SentinelConnectionConfig;
use Swoole\Coroutine;
use Swoole\Event;
use Swoole\Runtime;
use Swoole\Timer;
use Throwable;

use function array_merge;
use function Swoole\Coroutine\run;

trait CoroutineTestTrait
{
    public function runCoroWithPool(Closure $closure, ...$args): void
    {
        $this->runCoro(static function () use ($closure, $args) {
            $config = ConnectionConfig::fromArray(
                [
                    'host' => '127.0.0.1',
                    'port' => 6379,
                    'timeout' => 1.0,
                    'database' => 3,
                ]
            );

            $pool = new RedisPool($config);
            $pool->init();

            $closure(...array_merge([$pool], ...$args));
        });
    }

    public function runCoroWithSentinelPool(Closure $closure, ...$args): void
    {
        $this->runCoro(static function () use ($closure, $args) {
            $config = SentinelConnectionConfig::fromArray(
                [
                    'hosts' => ['127.0.0.1:26379'],
                    'timeout' => 1.0,
                    'database' => 3,
                ],
            );

            $pool = new RedisPool($config, new RedisSentinelConnector());
            $pool->init();

            $closure(...array_merge([$pool], ...$args));
        });
    }

    public function runCoro(Closure $closure, ...$args): void
    {
        Runtime::enableCoroutine();

        $result = new class {
            public ?\Throwable $ex = null;
        };

        run(
            Closure::fromCallable([$this, 'coroExec']),
            $closure,
            $result,
            ...$args
        );

        if ($result->ex instanceof Throwable) {
            throw $result->ex;
        }
    }

    private function coroExec(Closure $closure, $result, ...$args): void
    {
        Coroutine::defer(static function () {
            Event::exit();
            Timer::clearAll();
        });

        try {
            $closure(...$args);
        } catch (Throwable $e) {
            $result->ex = $e;
        }
    }
}
