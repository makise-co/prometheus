# prometheus swoole adapters
Prometheus [endclothing/prometheus_client_php](https://github.com/endclothing/prometheus_client_php) Storage Adapters for Swoole

### MakiseRedisAdaper
This adapter is a copy of original RedisAdapter, 
but it works with RedisPool ([makise/redis](https://github.com/makise-co/redis))
 and it's no longer work with static variables.
#### Usage
```php
/** @var \MakiseCo\Redis\RedisPool $pool */
$adapter = new \MakiseCo\Prometheus\Storage\MakiseRedisStorage($pool, 'test_makise');
$collector = new \Prometheus\CollectorRegistry($adapter);

// do anything what you want with $collector
```
