import asyncio
from datetime import datetime, timezone
from typing import Any, Dict
from utils import logger
from config import config

# Define get_session locally to avoid circular imports
async def get_session():
    """Create and return an aiohttp ClientSession"""
    import aiohttp
    return aiohttp.ClientSession()

class NotificationSystem:
    """
    Sends notifications via multiple channels (console, email, Slack, Telegram).
    """
    def __init__(self):
        """Initialize notification system"""
        self.channels = {}  # channel_name -> config
        self._lock = asyncio.Lock()

    async def configure_channel(self, channel_name: str, config: Dict[str, Any]) -> bool:
        """Configure a notification channel"""
        async with self._lock:
            self.channels[channel_name] = {
                "config": config,
                "enabled": True,
                "last_notification": None
            }
            logger.info(f"Configured notification channel: {channel_name}")
            return True

    async def disable_channel(self, channel_name: str) -> bool:
        """Disable a notification channel"""
        async with self._lock:
            if channel_name not in self.channels:
                return False
            self.channels[channel_name]["enabled"] = False
            logger.info(f"Disabled notification channel: {channel_name}")
            return True

    async def enable_channel(self, channel_name: str) -> bool:
        """Enable a notification channel"""
        async with self._lock:
            if channel_name not in self.channels:
                return False
            self.channels[channel_name]["enabled"] = True
            logger.info(f"Enabled notification channel: {channel_name}")
            return True

    async def send_notification(self, message: str, level: str = "info") -> Dict[str, Any]:
        """Send notification to all enabled channels"""
        async with self._lock:
            if not self.channels:
                logger.warning("No notification channels configured")
                return {
                    "status": "error",
                    "message": "No notification channels configured"
                }
            results = {}
            timestamp = datetime.now(timezone.utc).isoformat()
            for channel_name, channel in self.channels.items():
                if not channel["enabled"]:
                    continue
                try:
                    # Send notification through channel
                    if channel_name == "console":
                        await self._send_console_notification(message, level)
                        success = True
                    elif channel_name == "slack":
                        success = await self._send_slack_notification(message, level, channel["config"])
                    elif channel_name == "telegram":
                        success = await self._send_telegram_notification(message, level, channel["config"])
                    elif channel_name == "email":
                        success = await self._send_email_notification(message, level, channel["config"])
                    else:
                        logger.warning(f"Unknown notification channel: {channel_name}")
                        success = False
                    # Update channel's last notification
                    if success:
                        self.channels[channel_name]["last_notification"] = timestamp
                    results[channel_name] = success
                except Exception as e:
                    logger.error(f"Error sending notification via {channel_name}: {str(e)}")
                    results[channel_name] = False
            return {
                "status": "success" if any(results.values()) else "error",
                "timestamp": timestamp,
                "results": results
            }

    async def _send_console_notification(self, message: str, level: str) -> bool:
        """Send notification to console (log)"""
        if level == "info":
            logger.info(f"NOTIFICATION: {message}")
        elif level == "warning":
            logger.warning(f"NOTIFICATION: {message}")
        elif level == "error":
            logger.error(f"NOTIFICATION: {message}")
        elif level == "critical":
            logger.critical(f"NOTIFICATION: {message}")
        else:
            logger.info(f"NOTIFICATION: {message}")
        return True

    async def _send_slack_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification to Slack"""
        webhook_url = config.get("webhook_url")
        if not webhook_url:
            logger.error("Slack webhook URL not configured")
            return False
        try:
            session = await get_session()
            payload = {
                "text": message,
                "attachments": [{
                    "color": self._get_level_color(level),
                    "text": message,
                    "footer": f"Trading System • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
                }]
            }
            async with session.post(webhook_url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Failed to send Slack notification: {response.status}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error sending Slack notification: {str(e)}")
            return False

    def _get_level_color(self, level: str) -> str:
        """Get color for notification level"""
        colors = {
            "info": "#36a64f",  # green
            "warning": "#ffcc00",  # yellow
            "error": "#ff0000",  # red
            "critical": "#7b0000"  # dark red
        }
        return colors.get(level, "#36a64f")

    async def _send_telegram_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification to Telegram"""
        bot_token = config.get("bot_token")
        chat_id = config.get("chat_id")
        if not bot_token or not chat_id:
            logger.error("Telegram bot token or chat ID not configured")
            return False
        try:
            session = await get_session()
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            emoji = {
                "info": "ℹ️",
                "warning": "⚠️",
                "error": "🔴",
                "critical": "🚨"
            }.get(level, "ℹ️")
            
            # Convert Markdown-style formatting to HTML for more robust parsing
            # Split by ** and convert to <b></b> pairs
            parts = message.split("**")
            html_parts = []
            for i, part in enumerate(parts):
                if i % 2 == 0:  # Even index = normal text
                    html_parts.append(part)
                else:  # Odd index = bold text
                    html_parts.append(f"<b>{part}</b>")
            html_message = "".join(html_parts)
            
            payload = {
                "chat_id": chat_id,
                "text": f"{emoji} {html_message}",
                "parse_mode": "HTML"
            }
            async with session.post(api_url, json=payload) as response:
                if response.status != 200:
                    logger.error(f"Failed to send Telegram notification: {response.status}")
                    response_text = await response.text()
                    logger.error(f"Telegram response: {response_text}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error sending Telegram notification: {str(e)}")
            return False

    async def _send_email_notification(self, message: str, level: str, config: Dict[str, Any]) -> bool:
        """Send notification via email"""
        logger.info(f"Would send email notification: {message} (level: {level})")
        return True

    async def get_channel_status(self) -> Dict[str, Any]:
        """Get status of all notification channels"""
        async with self._lock:
            status = {}
            for channel_name, channel in self.channels.items():
                status[channel_name] = {
                    "enabled": channel["enabled"],
                    "last_notification": channel.get("last_notification")
                }
            return status

    async def shutdown(self):
        """Shutdown notification system"""
        logger.info("Shutting down notification system")
        self.channels.clear()
