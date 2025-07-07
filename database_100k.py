# database_100k.py - Dedicated database manager for 100k bot
import asyncio
import asyncpg
import logging
from datetime import datetime, timezone
from database import db_retry  # Reuse your existing retry decorator
from config import config

class Bot100kDatabaseManager:
    def __init__(self, db_url: str = None, schema: str = "bot_100k"):
        self.db_url = db_url or config.bot_100k_database_url or config.database_url
        self.schema = schema
        self.pool = None
        self.logger = logging.getLogger("bot_100k_db")

    async def initialize(self):
        """Initialize with dedicated schema for 100k bot"""
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=5,
                max_size=15,
                command_timeout=60.0,
                timeout=10.0,
            )
            
            # Create dedicated schema and tables
            await self._create_schema_and_tables()
            self.logger.info(f"100k Bot database initialized with schema: {self.schema}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize 100k bot database: {e}")
            raise

    async def _create_schema_and_tables(self):
        """Create dedicated schema and tables for 100k bot"""
        async with self.pool.acquire() as conn:
            # Create schema
            await conn.execute(f'CREATE SCHEMA IF NOT EXISTS {self.schema}')
            
            # Positions table with enhanced fields for live trading
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.schema}.positions (
                    position_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    entry_price DOUBLE PRECISION NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    open_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    close_time TIMESTAMP WITH TIME ZONE,
                    exit_price DOUBLE PRECISION,
                    current_price DOUBLE PRECISION NOT NULL,
                    pnl DOUBLE PRECISION NOT NULL,
                    pnl_percentage DOUBLE PRECISION NOT NULL,
                    status TEXT NOT NULL,
                    last_update TIMESTAMP WITH TIME ZONE NOT NULL,
                    metadata JSONB,
                    exit_reason TEXT,
                    
                    -- Live trading specific fields
                    account_id TEXT,
                    broker_transaction_id TEXT,
                    execution_latency_ms DOUBLE PRECISION,
                    slippage_pips DOUBLE PRECISION,
                    commission DOUBLE PRECISION,
                    weekend_age_hours DOUBLE PRECISION DEFAULT 0,
                    is_weekend_position BOOLEAN DEFAULT FALSE,
                    
                    -- Risk management fields
                    risk_percentage DOUBLE PRECISION,
                    portfolio_heat DOUBLE PRECISION,
                    correlation_group TEXT,
                    
                    -- Performance tracking
                    signal_source TEXT,
                    market_regime TEXT,
                    volatility_state TEXT
                )
            ''')
            
            # Trades execution log for high-frequency analysis
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.schema}.trade_executions (
                    id SERIAL PRIMARY KEY,
                    position_id TEXT,
                    execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    requested_price DOUBLE PRECISION,
                    executed_price DOUBLE PRECISION,
                    slippage_pips DOUBLE PRECISION,
                    latency_ms DOUBLE PRECISION,
                    broker_response JSONB,
                    success BOOLEAN NOT NULL,
                    error_message TEXT,
                    account_id TEXT
                )
            ''')
            
            # Risk events log
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.schema}.risk_events (
                    id SERIAL PRIMARY KEY,
                    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    event_type TEXT NOT NULL, -- 'limit_breach', 'correlation_warning', etc.
                    symbol TEXT,
                    position_id TEXT,
                    current_value DOUBLE PRECISION,
                    limit_value DOUBLE PRECISION,
                    action_taken TEXT,
                    metadata JSONB
                )
            ''')
            
            # Performance snapshots for reporting
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.schema}.performance_snapshots (
                    id SERIAL PRIMARY KEY,
                    snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    account_balance DOUBLE PRECISION NOT NULL,
                    total_pnl DOUBLE PRECISION NOT NULL,
                    open_positions_count INTEGER NOT NULL,
                    portfolio_heat DOUBLE PRECISION NOT NULL,
                    daily_pnl DOUBLE PRECISION,
                    drawdown DOUBLE PRECISION,
                    win_rate DOUBLE PRECISION,
                    avg_trade_duration_hours DOUBLE PRECISION,
                    metadata JSONB
                )
            ''')

            # Create indexes for performance
            indexes = [
                f'CREATE INDEX IF NOT EXISTS idx_{self.schema}_positions_symbol ON {self.schema}.positions(symbol)',
                f'CREATE INDEX IF NOT EXISTS idx_{self.schema}_positions_status ON {self.schema}.positions(status)',
                f'CREATE INDEX IF NOT EXISTS idx_{self.schema}_positions_open_time ON {self.schema}.positions(open_time)',
                f'CREATE INDEX IF NOT EXISTS idx_{self.schema}_executions_time ON {self.schema}.trade_executions(execution_time)',
                f'CREATE INDEX IF NOT EXISTS idx_{self.schema}_risk_events_time ON {self.schema}.risk_events(event_time)',
            ]
            
            for index_sql in indexes:
                await conn.execute(index_sql)

    @db_retry()
    async def save_position(self, position_data: dict) -> bool:
        """Save position with 100k bot specific fields"""
        try:
            async with self.pool.acquire() as conn:
                # Enhanced position data for live trading
                columns = list(position_data.keys())
                placeholders = [f"${i+1}" for i in range(len(columns))]
                
                query = f'''
                    INSERT INTO {self.schema}.positions ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (position_id) 
                    DO UPDATE SET {', '.join(f'{col} = EXCLUDED.{col}' for col in columns)}
                '''
                
                values = [position_data[col] for col in columns]
                await conn.execute(query, *values)
                return True
                
        except Exception as e:
            self.logger.error(f"Error saving position to 100k bot database: {e}")
            return False

    @db_retry()
    async def log_trade_execution(self, execution_data: dict) -> bool:
        """Log trade execution details for analysis"""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f'''
                    INSERT INTO {self.schema}.trade_executions 
                    (position_id, execution_time, symbol, action, size, requested_price, 
                     executed_price, slippage_pips, latency_ms, broker_response, success, 
                     error_message, account_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ''', 
                execution_data.get('position_id'),
                execution_data.get('execution_time', datetime.now(timezone.utc)),
                execution_data.get('symbol'),
                execution_data.get('action'),
                execution_data.get('size'),
                execution_data.get('requested_price'),
                execution_data.get('executed_price'),
                execution_data.get('slippage_pips'),
                execution_data.get('latency_ms'),
                execution_data.get('broker_response'),
                execution_data.get('success'),
                execution_data.get('error_message'),
                execution_data.get('account_id')
                )
                return True
                
        except Exception as e:
            self.logger.error(f"Error logging trade execution: {e}")
            return False

    async def get_performance_summary(self, days: int = 30) -> dict:
        """Get comprehensive performance summary for the 100k bot"""
        try:
            async with self.pool.acquire() as conn:
                # Get latest snapshot
                latest_snapshot = await conn.fetchrow(f'''
                    SELECT * FROM {self.schema}.performance_snapshots 
                    ORDER BY snapshot_time DESC LIMIT 1
                ''')
                
                # Get recent trade statistics
                trade_stats = await conn.fetchrow(f'''
                    SELECT 
                        COUNT(*) as total_trades,
                        AVG(slippage_pips) as avg_slippage,
                        AVG(latency_ms) as avg_latency,
                        SUM(CASE WHEN success THEN 1 ELSE 0 END)::float / COUNT(*) as success_rate
                    FROM {self.schema}.trade_executions 
                    WHERE execution_time > NOW() - INTERVAL '{days} days'
                ''')
                
                # Get risk events
                risk_events = await conn.fetch(f'''
                    SELECT event_type, COUNT(*) as count 
                    FROM {self.schema}.risk_events 
                    WHERE event_time > NOW() - INTERVAL '{days} days'
                    GROUP BY event_type
                ''')
                
                return {
                    'latest_snapshot': dict(latest_snapshot) if latest_snapshot else None,
                    'trade_statistics': dict(trade_stats) if trade_stats else None,
                    'risk_events': {row['event_type']: row['count'] for row in risk_events},
                    'analysis_period_days': days
                }
                
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return {}

    async def backup_100k_data(self, backup_path: str) -> bool:
        """Create backup of 100k bot data"""
        try:
            # Use pg_dump with schema-specific backup
            import subprocess
            from urllib.parse import urlparse
            
            parsed_url = urlparse(self.db_url)
            
            cmd = [
                'pg_dump',
                f"--host={parsed_url.hostname}",
                f"--port={parsed_url.port or 5432}",
                f"--username={parsed_url.username}",
                f"--dbname={parsed_url.path.lstrip('/')}",
                f"--schema={self.schema}",
                '--format=custom',
                f"--file={backup_path}",
            ]
            
            env = os.environ.copy()
            env['PGPASSWORD'] = parsed_url.password
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info(f"100k bot data backup successful: {backup_path}")
                return True
            else:
                self.logger.error(f"100k bot backup failed: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error creating 100k bot backup: {e}")
            return False