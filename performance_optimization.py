"""
INSTITUTIONAL-GRADE PERFORMANCE OPTIMIZATION
Redis caching, message queuing, and database read replicas
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
import logging
from dataclasses import dataclass, asdict
from enum import Enum
import aioredis
import aiokafka
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncpg
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

class CacheStrategy(Enum):
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    CACHE_ASIDE = "cache_aside"

@dataclass
class CacheConfig:
    """Redis cache configuration"""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_connections: int = 20
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    retry_on_timeout: bool = True
    decode_responses: bool = True

@dataclass
class MessageQueueConfig:
    """Kafka message queue configuration"""
    bootstrap_servers: List[str] = None
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    group_id: str = "trading_bot_group"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True

class RedisCache:
    """High-performance Redis cache with institutional-grade features"""
    
    def __init__(self, config: CacheConfig):
        self.config = config
        self.redis_pool = None
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize Redis connection pool"""
        try:
            self.redis_pool = aioredis.ConnectionPool.from_url(
                f"redis://{self.config.host}:{self.config.port}/{self.config.db}",
                password=self.config.password,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                retry_on_timeout=self.config.retry_on_timeout,
                decode_responses=self.config.decode_responses
            )
            logger.info("✅ Redis cache initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Redis cache: {e}")
            raise
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            async with aioredis.Redis(connection_pool=self.redis_pool) as redis:
                value = await redis.get(key)
                if value:
                    return json.loads(value)
                return None
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """Set value in cache with TTL"""
        try:
            async with aioredis.Redis(connection_pool=self.redis_pool) as redis:
                serialized_value = json.dumps(value, default=str)
                await redis.setex(key, ttl, serialized_value)
                return True
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        try:
            async with aioredis.Redis(connection_pool=self.redis_pool) as redis:
                result = await redis.delete(key)
                return result > 0
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def get_or_set(self, key: str, factory_func, ttl: int = 3600) -> Any:
        """Get from cache or set using factory function"""
        value = await self.get(key)
        if value is not None:
            return value
        
        # Generate value using factory function
        value = await factory_func() if asyncio.iscoroutinefunction(factory_func) else factory_func()
        
        # Cache the value
        await self.set(key, value, ttl)
        return value
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern"""
        try:
            async with aioredis.Redis(connection_pool=self.redis_pool) as redis:
                keys = await redis.keys(pattern)
                if keys:
                    return await redis.delete(*keys)
                return 0
        except Exception as e:
            logger.error(f"Cache pattern invalidation error for {pattern}: {e}")
            return 0

class MessageQueue:
    """High-throughput message queue using Kafka"""
    
    def __init__(self, config: MessageQueueConfig):
        self.config = config
        self.producer = None
        self.consumer = None
        self._running = False
        
    async def initialize(self):
        """Initialize Kafka producer and consumer"""
        try:
            # Initialize producer
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers or ["localhost:9092"],
                security_protocol=self.config.security_protocol,
                sasl_mechanism=self.config.sasl_mechanism,
                sasl_plain_username=self.config.sasl_username,
                sasl_plain_password=self.config.sasl_password
            )
            await self.producer.start()
            
            # Initialize consumer
            self.consumer = AIOKafkaConsumer(
                "trading_alerts",
                "risk_events",
                "position_updates",
                "system_events",
                bootstrap_servers=self.config.bootstrap_servers or ["localhost:9092"],
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                security_protocol=self.config.security_protocol,
                sasl_mechanism=self.config.sasl_mechanism,
                sasl_plain_username=self.config.sasl_username,
                sasl_plain_password=self.config.sasl_password
            )
            
            logger.info("✅ Message queue initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize message queue: {e}")
            raise
    
    async def publish_alert(self, alert_data: Dict[str, Any], topic: str = "trading_alerts"):
        """Publish trading alert to queue"""
        try:
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": "trading_alert",
                "data": alert_data
            }
            
            await self.producer.send_and_wait(
                topic,
                json.dumps(message).encode('utf-8')
            )
            logger.debug(f"Published alert to {topic}: {alert_data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to publish alert: {e}")
    
    async def publish_risk_event(self, risk_data: Dict[str, Any]):
        """Publish risk event to queue"""
        try:
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": "risk_event",
                "data": risk_data
            }
            
            await self.producer.send_and_wait(
                "risk_events",
                json.dumps(message).encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Failed to publish risk event: {e}")
    
    async def start_consuming(self, message_handler):
        """Start consuming messages from queue"""
        try:
            await self.consumer.start()
            self._running = True
            
            async for msg in self.consumer:
                try:
                    message_data = json.loads(msg.value.decode('utf-8'))
                    await message_handler(msg.topic, message_data)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except Exception as e:
            logger.error(f"Error in message consumption: {e}")
        finally:
            self._running = False
    
    async def stop(self):
        """Stop producer and consumer"""
        self._running = False
        if self.producer:
            await self.producer.stop()
        if self.consumer:
            await self.consumer.stop()

class DatabaseReadReplicas:
    """Database read replicas for analytics and reporting"""
    
    def __init__(self, primary_config: Dict[str, Any], replica_configs: List[Dict[str, Any]]):
        self.primary_config = primary_config
        self.replica_configs = replica_configs
        self.primary_pool = None
        self.replica_pools = []
        self._replica_index = 0
        
    async def initialize(self):
        """Initialize database connection pools"""
        try:
            # Initialize primary database
            self.primary_pool = await asyncpg.create_pool(
                **self.primary_config,
                min_size=5,
                max_size=20
            )
            
            # Initialize replica databases
            for replica_config in self.replica_configs:
                replica_pool = await asyncpg.create_pool(
                    **replica_config,
                    min_size=3,
                    max_size=10
                )
                self.replica_pools.append(replica_pool)
            
            logger.info(f"✅ Database pools initialized: 1 primary, {len(self.replica_pools)} replicas")
        except Exception as e:
            logger.error(f"❌ Failed to initialize database pools: {e}")
            raise
    
    async def get_read_connection(self):
        """Get connection from read replica (round-robin)"""
        if not self.replica_pools:
            # Fallback to primary if no replicas
            return self.primary_pool.acquire()
        
        # Round-robin selection
        pool = self.replica_pools[self._replica_index % len(self.replica_pools)]
        self._replica_index += 1
        return pool.acquire()
    
    async def get_write_connection(self):
        """Get connection from primary database"""
        return self.primary_pool.acquire()
    
    @asynccontextmanager
    async def read_transaction(self):
        """Context manager for read transactions"""
        async with self.get_read_connection() as conn:
            async with conn.transaction():
                yield conn
    
    @asynccontextmanager
    async def write_transaction(self):
        """Context manager for write transactions"""
        async with self.get_write_connection() as conn:
            async with conn.transaction():
                yield conn

class PerformanceOptimizer:
    """Main performance optimization coordinator"""
    
    def __init__(self):
        self.cache = None
        self.message_queue = None
        self.database_replicas = None
        self._initialized = False
        
    async def initialize(self, 
                        cache_config: CacheConfig = None,
                        mq_config: MessageQueueConfig = None,
                        db_config: Dict[str, Any] = None):
        """Initialize all performance components"""
        try:
            # Initialize Redis cache
            if cache_config:
                self.cache = RedisCache(cache_config)
                await self.cache.initialize()
            
            # Initialize message queue
            if mq_config:
                self.message_queue = MessageQueue(mq_config)
                await self.message_queue.initialize()
            
            # Initialize database replicas
            if db_config:
                self.database_replicas = DatabaseReadReplicas(
                    db_config.get("primary", {}),
                    db_config.get("replicas", [])
                )
                await self.database_replicas.initialize()
            
            self._initialized = True
            logger.info("✅ Performance optimizer initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize performance optimizer: {e}")
            raise
    
    async def cache_market_data(self, symbol: str, data: Dict[str, Any], ttl: int = 60):
        """Cache market data with short TTL"""
        if not self.cache:
            return False
        
        key = f"market_data:{symbol}"
        return await self.cache.set(key, data, ttl)
    
    async def get_cached_market_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get cached market data"""
        if not self.cache:
            return None
        
        key = f"market_data:{symbol}"
        return await self.cache.get(key)
    
    async def cache_position_data(self, position_id: str, data: Dict[str, Any], ttl: int = 300):
        """Cache position data with medium TTL"""
        if not self.cache:
            return False
        
        key = f"position:{position_id}"
        return await self.cache.set(key, data, ttl)
    
    async def get_cached_position_data(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get cached position data"""
        if not self.cache:
            return None
        
        key = f"position:{position_id}"
        return await self.cache.get(key)
    
    async def publish_trading_alert(self, alert_data: Dict[str, Any]):
        """Publish trading alert to queue"""
        if not self.message_queue:
            return False
        
        return await self.message_queue.publish_alert(alert_data)
    
    async def publish_risk_event(self, risk_data: Dict[str, Any]):
        """Publish risk event to queue"""
        if not self.message_queue:
            return False
        
        return await self.message_queue.publish_risk_event(risk_data)
    
    async def get_analytics_data(self, query: str, params: tuple = ()):
        """Execute analytics query on read replica"""
        if not self.database_replicas:
            return None
        
        async with self.database_replicas.read_transaction() as conn:
            return await conn.fetch(query, *params)
    
    async def execute_write_operation(self, query: str, params: tuple = ()):
        """Execute write operation on primary database"""
        if not self.database_replicas:
            return None
        
        async with self.database_replicas.write_transaction() as conn:
            return await conn.execute(query, *params)

# Global performance optimizer instance
performance_optimizer = PerformanceOptimizer()

# Convenience functions
async def cache_market_data(symbol: str, data: Dict[str, Any], ttl: int = 60):
    """Cache market data"""
    await performance_optimizer.cache_market_data(symbol, data, ttl)

async def get_cached_market_data(symbol: str) -> Optional[Dict[str, Any]]:
    """Get cached market data"""
    return await performance_optimizer.get_cached_market_data(symbol)

async def publish_trading_alert(alert_data: Dict[str, Any]):
    """Publish trading alert"""
    await performance_optimizer.publish_trading_alert(alert_data)

async def publish_risk_event(risk_data: Dict[str, Any]):
    """Publish risk event"""
    await performance_optimizer.publish_risk_event(risk_data)
