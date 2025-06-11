import os
import json
import tarfile
import glob
import re
import shutil
import asyncio
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
import alert_handler
from config import config
from utils import logger

class BackupManager:
    """
    Manages database and position data backups
    """
    def __init__(self, db_manager=None):
        """Initialize backup manager"""
        self.db_manager = db_manager
        self.backup_dir = config.backup_dir
        self.last_backup_time = None
        self._lock = asyncio.Lock()
        os.makedirs(self.backup_dir, exist_ok=True)

    async def create_backup(self, include_market_data: bool = False, compress: bool = True) -> bool:
        async with self._lock:
            try:
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                backup_basename = f"trading_system_backup_{timestamp}"
                backup_subdir = os.path.join(self.backup_dir, backup_basename)
                os.makedirs(backup_subdir, exist_ok=True)
                if self.db_manager:
                    db_backup_path = os.path.join(backup_subdir, "database.db")
                    db_backed_up = await self.db_manager.backup_database(db_backup_path)
                    if not db_backed_up:
                        logger.error("Failed to backup database")
                        return False
                if 'alert_handler' in globals() and alert_handler and hasattr(alert_handler, 'position_tracker'):
                    all_positions = await alert_handler.position_tracker.get_all_positions()
                    positions_backup_path = os.path.join(backup_subdir, "positions.json")
                    with open(positions_backup_path, 'w') as f:
                        json.dump(all_positions, f, indent=2)
                    logger.info(f"Backed up {len(all_positions)} positions to {positions_backup_path}")
                if include_market_data and 'alert_handler' in globals() and alert_handler:
                    market_data = {}
                    if hasattr(alert_handler, 'volatility_monitor'):
                        market_data['volatility'] = alert_handler.volatility_monitor.get_all_volatility_states()
                    if hasattr(alert_handler, 'regime_classifier'):
                        market_data['regimes'] = {}
                        if hasattr(alert_handler.regime_classifier, 'regimes'):
                            for symbol, regime_data in alert_handler.regime_classifier.regimes.items():
                                if isinstance(regime_data.get('last_update'), datetime):
                                    regime_data = regime_data.copy()
                                    regime_data['last_update'] = regime_data['last_update'].isoformat()
                                market_data['regimes'][symbol] = regime_data
                    market_data_path = os.path.join(backup_subdir, "market_data.json")
                    with open(market_data_path, 'w') as f:
                        json.dump(market_data, f, indent=2)
                    logger.info(f"Backed up market data to {market_data_path}")
                if compress:
                    archive_path = os.path.join(self.backup_dir, f"{backup_basename}.tar.gz")
                    with tarfile.open(archive_path, "w:gz") as tar:
                        tar.add(backup_subdir, arcname=os.path.basename(backup_subdir))
                    shutil.rmtree(backup_subdir)
                    logger.info(f"Created compressed backup at {archive_path}")
                self.last_backup_time = datetime.now(timezone.utc)
                return True
            except Exception as e:
                logger.error(f"Error creating backup: {str(e)}")
                logger.error(traceback.format_exc())
                return False

    async def list_backups(self) -> List[Dict[str, Any]]:
        try:
            backups = []
            compressed_pattern = os.path.join(self.backup_dir, "trading_system_backup_*.tar.gz")
            for backup_path in glob.glob(compressed_pattern):
                filename = os.path.basename(backup_path)
                timestamp_str = re.search(r"trading_system_backup_(\d+_\d+)", filename)
                timestamp = None
                if timestamp_str:
                    try:
                        timestamp = datetime.strptime(timestamp_str.group(1), "%Y%m%d_%H%M%S")
                    except ValueError:
                        pass
                backups.append({
                    "filename": filename,
                    "path": backup_path,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                    "size": os.path.getsize(backup_path),
                    "type": "compressed"
                })
            uncompressed_pattern = os.path.join(self.backup_dir, "trading_system_backup_*")
            for backup_path in glob.glob(uncompressed_pattern):
                if os.path.isdir(backup_path) and not backup_path.endswith(".tar.gz"):
                    dirname = os.path.basename(backup_path)
                    timestamp_str = re.search(r"trading_system_backup_(\d+_\d+)", dirname)
                    timestamp = None
                    if timestamp_str:
                        try:
                            timestamp = datetime.strptime(timestamp_str.group(1), "%Y%m%d_%H%M%S")
                        except ValueError:
                            pass
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(backup_path):
                        for f in filenames:
                            fp = os.path.join(dirpath, f)
                            total_size += os.path.getsize(fp)
                    backups.append({
                        "filename": dirname,
                        "path": backup_path,
                        "timestamp": timestamp.isoformat() if timestamp else None,
                        "size": total_size,
                        "type": "directory"
                    })
            backups.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            return backups
        except Exception as e:
            logger.error(f"Error listing backups: {str(e)}")
            return []

    async def cleanup_old_backups(self, max_age_days: int = 30, keep_min: int = 5):
        try:
            backups = await self.list_backups()
            backups.sort(key=lambda x: x.get("timestamp", ""))
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
            backups_to_delete = []
            for backup in backups[:-keep_min] if len(backups) > keep_min else []:
                if backup.get("timestamp", "") < cutoff_date:
                    backups_to_delete.append(backup)
            for backup in backups_to_delete:
                path = backup["path"]
                try:
                    if backup["type"] == "compressed":
                        os.remove(path)
                    else:
                        shutil.rmtree(path)
                    logger.info(f"Removed old backup: {os.path.basename(path)}")
                except Exception as e:
                    logger.error(f"Error removing backup {path}: {str(e)}")
            return len(backups_to_delete)
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {str(e)}")
            return 0

    async def schedule_backups(self, interval_hours: int = 24):
        logger.info(f"Scheduling automatic backups every {interval_hours} hours")
        while True:
            try:
                success = await self.create_backup(include_market_data=True, compress=True)
                if success:
                    logger.info("Automatic backup created successfully")
                    deleted_count = await self.cleanup_old_backups()
                    if deleted_count > 0:
                        logger.info(f"Cleaned up {deleted_count} old backups")
                await asyncio.sleep(interval_hours * 3600)
            except Exception as e:
                logger.error(f"Error in scheduled backup: {str(e)}")
                await asyncio.sleep(3600)  # 1 hour 
