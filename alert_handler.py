import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Union, Callable

logger = logging.getLogger("fx-trading-bridge")

class AlertPriority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertCategory(Enum):
    SYSTEM = "system"
    POSITION = "position"
    RISK = "risk"
    MARKET = "market"
    TECHNICAL = "technical"
    EXTERNAL = "external"
    AUTHENTICATION = "authentication"
    PERFORMANCE = "performance"

class Alert:
    def __init__(
        self,
        message: str,
        category: AlertCategory,
        priority: AlertPriority = AlertPriority.MEDIUM,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None
    ):
        self.message = message
        self.category = category
        self.priority = priority
        self.position_id = position_id
        self.symbol = symbol
        self.additional_data = additional_data or {}
        self.timestamp = timestamp or datetime.utcnow()
        self.alert_id = f"{self.timestamp.strftime('%Y%m%d%H%M%S')}-{id(self)}"
        self.acknowledged = False
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary for JSON serialization"""
        return {
            "alert_id": self.alert_id,
            "message": self.message,
            "category": self.category.value,
            "priority": self.priority.value,
            "position_id": self.position_id,
            "symbol": self.symbol,
            "additional_data": self.additional_data,
            "timestamp": self.timestamp.isoformat(),
            "acknowledged": self.acknowledged
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Alert':
        """Create an Alert instance from a dictionary"""
        alert = cls(
            message=data["message"],
            category=AlertCategory(data["category"]),
            priority=AlertPriority(data["priority"]),
            position_id=data.get("position_id"),
            symbol=data.get("symbol"),
            additional_data=data.get("additional_data", {}),
            timestamp=datetime.fromisoformat(data["timestamp"])
        )
        alert.alert_id = data["alert_id"]
        alert.acknowledged = data.get("acknowledged", False)
        return alert

class AlertHandler:
    """
    Manages system and trading alerts with notification capabilities.
    
    Features:
    - Alert categorization and prioritization
    - Alert persistence and retrieval
    - Integration with notification systems
    - Alert filtering and subscription capabilities
    - Alert acknowledgment and resolution tracking
    """
    
    def __init__(self):
        self.alerts: List[Alert] = []
        self.alert_history: List[Alert] = []  # For storing older alerts
        self.history_max_size = 1000
        self.current_alerts_max_size = 100
        self.notification_handlers: Dict[AlertCategory, List[Callable]] = {
            category: [] for category in AlertCategory
        }
        self.filters: Dict[str, Callable] = {}
        self.lock = asyncio.Lock()
        logger.info("AlertHandler initialized")
        
    async def add_alert(
        self,
        message: str,
        category: AlertCategory,
        priority: AlertPriority = AlertPriority.MEDIUM,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None,
        notify: bool = True
    ) -> Alert:
        """
        Create and register a new alert in the system.
        
        Args:
            message: Alert message text
            category: Category of the alert
            priority: Priority level of the alert
            position_id: Related position ID (if applicable)
            symbol: Related trading symbol (if applicable)
            additional_data: Any additional contextual data for the alert
            notify: Whether to trigger notifications for this alert
            
        Returns:
            The created Alert object
        """
        alert = Alert(
            message=message,
            category=category,
            priority=priority,
            position_id=position_id,
            symbol=symbol,
            additional_data=additional_data
        )
        
        async with self.lock:
            # Add to current alerts and manage size
            self.alerts.append(alert)
            if len(self.alerts) > self.current_alerts_max_size:
                oldest_alert = self.alerts.pop(0)
                self.alert_history.append(oldest_alert)
                
            # Manage history size
            if len(self.alert_history) > self.history_max_size:
                self.alert_history = self.alert_history[-self.history_max_size:]
        
        # Log the alert based on priority
        if priority == AlertPriority.CRITICAL:
            logger.critical(f"ALERT: {message} [{category.value}]")
        elif priority == AlertPriority.HIGH:
            logger.error(f"ALERT: {message} [{category.value}]")
        elif priority == AlertPriority.MEDIUM:
            logger.warning(f"ALERT: {message} [{category.value}]")
        else:
            logger.info(f"ALERT: {message} [{category.value}]")
            
        # Process notifications if enabled
        if notify:
            await self._process_notifications(alert)
            
        return alert
    
    async def _process_notifications(self, alert: Alert) -> None:
        """Process notifications for an alert using registered handlers"""
        # Call category-specific handlers
        for handler in self.notification_handlers.get(alert.category, []):
            try:
                await handler(alert)
            except Exception as e:
                logger.error(f"Error in notification handler: {str(e)}")
                
        # Call global handlers (if any)
        for handler in self.notification_handlers.get(None, []):
            try:
                await handler(alert)
            except Exception as e:
                logger.error(f"Error in global notification handler: {str(e)}")
    
    def register_notification_handler(
        self,
        handler: Callable[[Alert], Any],
        categories: Optional[List[AlertCategory]] = None
    ) -> None:
        """
        Register a notification handler function for specific alert categories
        
        Args:
            handler: Async callable that accepts an Alert parameter
            categories: List of alert categories to handle, None for all categories
        """
        if categories is None:
            # Register for all categories
            for category in AlertCategory:
                self.notification_handlers[category].append(handler)
        else:
            # Register for specific categories
            for category in categories:
                self.notification_handlers[category].append(handler)
    
    async def get_alerts(
        self,
        include_acknowledged: bool = False,
        category: Optional[AlertCategory] = None,
        priority: Optional[AlertPriority] = None,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        limit: int = 50,
        include_history: bool = False
    ) -> List[Alert]:
        """
        Get alerts filtered by various criteria
        
        Args:
            include_acknowledged: Whether to include acknowledged alerts
            category: Filter by alert category
            priority: Filter by alert priority
            position_id: Filter by position ID
            symbol: Filter by trading symbol
            limit: Maximum number of alerts to return
            include_history: Whether to include historical alerts
            
        Returns:
            List of filtered alerts
        """
        async with self.lock:
            # Determine which collections to search
            alert_collections = [self.alerts]
            if include_history:
                alert_collections.append(self.alert_history)
                
            # Flatten and filter alerts
            all_alerts = []
            for collection in alert_collections:
                for alert in collection:
                    # Apply filters
                    if not include_acknowledged and alert.acknowledged:
                        continue
                    if category is not None and alert.category != category:
                        continue
                    if priority is not None and alert.priority != priority:
                        continue
                    if position_id is not None and alert.position_id != position_id:
                        continue
                    if symbol is not None and alert.symbol != symbol:
                        continue
                    
                    all_alerts.append(alert)
            
            # Sort by timestamp (newest first) and apply limit
            all_alerts.sort(key=lambda a: a.timestamp, reverse=True)
            return all_alerts[:limit]
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """
        Mark an alert as acknowledged
        
        Args:
            alert_id: ID of the alert to acknowledge
            
        Returns:
            True if alert was found and acknowledged, False otherwise
        """
        async with self.lock:
            # Search in current alerts
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.acknowledged = True
                    logger.info(f"Alert {alert_id} acknowledged")
                    return True
            
            # If not found, search in history
            for alert in self.alert_history:
                if alert.alert_id == alert_id:
                    alert.acknowledged = True
                    logger.info(f"Historical alert {alert_id} acknowledged")
                    return True
            
            logger.warning(f"Alert {alert_id} not found for acknowledgment")
            return False
    
    async def clear_acknowledged_alerts(self) -> int:
        """
        Move acknowledged alerts from current alerts to history
        
        Returns:
            Number of alerts moved to history
        """
        moved_count = 0
        async with self.lock:
            # Find acknowledged alerts
            acknowledged = [a for a in self.alerts if a.acknowledged]
            
            # Update collections
            self.alerts = [a for a in self.alerts if not a.acknowledged]
            self.alert_history.extend(acknowledged)
            moved_count = len(acknowledged)
            
            # Trim history if needed
            if len(self.alert_history) > self.history_max_size:
                self.alert_history = self.alert_history[-self.history_max_size:]
        
        logger.info(f"Moved {moved_count} acknowledged alerts to history")
        return moved_count
    
    async def add_system_alert(
        self,
        message: str,
        priority: AlertPriority = AlertPriority.MEDIUM,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> Alert:
        """Convenience method to add a system alert"""
        return await self.add_alert(
            message=message,
            category=AlertCategory.SYSTEM,
            priority=priority,
            additional_data=additional_data
        )
    
    async def add_position_alert(
        self,
        message: str,
        position_id: str,
        symbol: str,
        priority: AlertPriority = AlertPriority.MEDIUM,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> Alert:
        """Convenience method to add a position-related alert"""
        return await self.add_alert(
            message=message,
            category=AlertCategory.POSITION,
            priority=priority,
            position_id=position_id,
            symbol=symbol,
            additional_data=additional_data
        )
    
    async def add_risk_alert(
        self,
        message: str,
        priority: AlertPriority = AlertPriority.HIGH,
        position_id: Optional[str] = None,
        symbol: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> Alert:
        """Convenience method to add a risk-related alert"""
        return await self.add_alert(
            message=message,
            category=AlertCategory.RISK,
            priority=priority,
            position_id=position_id,
            symbol=symbol,
            additional_data=additional_data
        )
    
    async def export_alerts(self, include_history: bool = True) -> Dict[str, Any]:
        """
        Export all alerts as a serializable dictionary
        
        Args:
            include_history: Whether to include historical alerts
            
        Returns:
            Dictionary with current and historical alerts
        """
        async with self.lock:
            result = {
                "current_alerts": [a.to_dict() for a in self.alerts],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if include_history:
                result["alert_history"] = [a.to_dict() for a in self.alert_history]
                
            return result
    
    async def import_alerts(self, data: Dict[str, Any]) -> None:
        """
        Import alerts from a dictionary (previously exported)
        
        Args:
            data: Dictionary with alert data
        """
        async with self.lock:
            # Import current alerts
            if "current_alerts" in data:
                self.alerts = [
                    Alert.from_dict(alert_data) 
                    for alert_data in data["current_alerts"]
                ]
            
            # Import alert history
            if "alert_history" in data:
                self.alert_history = [
                    Alert.from_dict(alert_data) 
                    for alert_data in data["alert_history"]
                ]
            
            logger.info(f"Imported {len(self.alerts)} current alerts and {len(self.alert_history)} historical alerts") 