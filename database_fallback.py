import sqlite3
import os
from database import DatabaseManager

class EnhancedDatabaseManager(DatabaseManager):
    async def initialize_with_fallback(self):
        """Initialize with PostgreSQL + SQLite fallback"""
        try:
            await super().initialize()
            logger.info("✅ Database initialized successfully")
        except Exception as e:
            logger.warning(f"PostgreSQL failed: {e}, using SQLite fallback")
            self.db_type = "sqlite"
            self.db_url = "sqlite:///trading_bot.db"
            await self._initialize_sqlite()