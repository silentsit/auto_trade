"""
Exchange Adapter Module for FX Trading Bridge

Provides a standardized interface for interacting with different trading exchanges
while abstracting away exchange-specific implementation details.
"""

import logging
import asyncio
import aiohttp
import hmac
import hashlib
import time as time_module
import urllib.parse
from typing import Dict, List, Optional, Any, Union
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class OrderType(str, Enum):
    """Standardized order types across exchanges"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderSide(str, Enum):
    """Standardized order sides across exchanges"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Standardized order statuses across exchanges"""
    PENDING = "pending"
    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class Order(BaseModel):
    """Standardized order model"""
    symbol: str
    order_type: OrderType
    side: OrderSide
    quantity: float
    price: Optional[float] = None
    stop_price: Optional[float] = None
    time_in_force: str = "GTC"  # Good Till Canceled
    client_order_id: Optional[str] = None
    exchange_order_id: Optional[str] = None
    status: OrderStatus = OrderStatus.PENDING
    created_at: Optional[datetime] = None
    
    class Config:
        extra = "allow"  # Allow extra fields for exchange-specific data


class ExchangeAdapter(ABC):
    """Base exchange adapter interface"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the exchange adapter with configuration
        
        Args:
            config: Exchange-specific configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self.session = None
        self.rate_limit_semaphore = asyncio.Semaphore(5)  # Default rate limit
    
    async def initialize(self):
        """Initialize adapter resources"""
        if not self.session:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(
                    total=self.config.get("timeout", 30)
                )
            )
        self.logger.info(f"Initialized {self.__class__.__name__}")
    
    async def shutdown(self):
        """Clean up adapter resources"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
        self.logger.info(f"Shutdown {self.__class__.__name__}")
    
    async def _rate_limited_request(self, coro):
        """Execute a coroutine with rate limiting"""
        async with self.rate_limit_semaphore:
            result = await coro
            await asyncio.sleep(self.config.get("rate_limit_delay", 0.2))
            return result
    
    @abstractmethod
    async def get_account_info(self) -> Dict[str, Any]:
        """Get account information"""
        pass
    
    @abstractmethod
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get current ticker information"""
        pass
    
    @abstractmethod
    async def get_orderbook(self, symbol: str, depth: int = 10) -> Dict[str, Any]:
        """Get order book for a symbol"""
        pass
    
    @abstractmethod
    async def get_candles(
        self, symbol: str, timeframe: str, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical candles"""
        pass
    
    @abstractmethod
    async def place_order(self, order: Order) -> Order:
        """Place a new order"""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an existing order"""
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        pass
    
    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str, amount: Optional[float] = None) -> bool:
        """Close a position for a symbol"""
        pass


class OandaAdapter(ExchangeAdapter):
    """Oanda exchange adapter implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_url = config.get("api_url", "https://api-fxtrade.oanda.com/v3")
        self.account_id = config.get("account_id")
        self.api_token = config.get("api_token")
        
        if not self.account_id or not self.api_token:
            raise ValueError("Oanda adapter requires account_id and api_token in config")
        
        # Setup rate limits specific to Oanda
        self.rate_limit_semaphore = asyncio.Semaphore(
            config.get("rate_limit_max_concurrent", 4)
        )
    
    async def get_account_info(self) -> Dict[str, Any]:
        """Get Oanda account information"""
        endpoint = f"{self.api_url}/accounts/{self.account_id}"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        
        try:
            async with self._rate_limited_request(self.session.get(endpoint, headers=headers)) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "balance": float(data["account"]["balance"]),
                        "currency": data["account"]["currency"],
                        "margin_available": float(data["account"]["marginAvailable"]),
                        "open_trade_count": data["account"]["openTradeCount"],
                        "open_position_count": data["account"]["openPositionCount"],
                        "unrealized_pl": float(data["account"]["unrealizedPL"]),
                        "nav": float(data["account"]["NAV"])
                    }
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get account info: {error_msg}")
                    return {"error": error_msg, "status_code": response.status}
        except Exception as e:
            self.logger.exception(f"Error fetching account info: {str(e)}")
            return {"error": str(e)}
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get current ticker for a symbol from Oanda"""
        endpoint = f"{self.api_url}/instruments/{symbol}/candles"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {
            "count": 1,
            "price": "M",  # Midpoint candles
            "granularity": "S5"  # 5-second candles for latest price
        }
        
        try:
            async with self._rate_limited_request(
                self.session.get(endpoint, headers=headers, params=params)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data["candles"] and len(data["candles"]) > 0:
                        latest = data["candles"][0]
                        return {
                            "symbol": symbol,
                            "bid": float(latest["mid"]["c"]) - 0.0001,  # Approximate bid
                            "ask": float(latest["mid"]["c"]) + 0.0001,  # Approximate ask
                            "price": float(latest["mid"]["c"]),
                            "time": latest["time"]
                        }
                    return {"symbol": symbol, "error": "No data available"}
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get ticker for {symbol}: {error_msg}")
                    return {"symbol": symbol, "error": error_msg}
        except Exception as e:
            self.logger.exception(f"Error fetching ticker for {symbol}: {str(e)}")
            return {"symbol": symbol, "error": str(e)}
    
    async def get_orderbook(self, symbol: str, depth: int = 10) -> Dict[str, Any]:
        """Get order book for a symbol"""
        # Oanda doesn't provide a traditional orderbook API
        # Approximating with price ladder
        endpoint = f"{self.api_url}/instruments/{symbol}/orderBook"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        
        try:
            async with self._rate_limited_request(
                self.session.get(endpoint, headers=headers)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "symbol": symbol,
                        "bids": [{"price": float(level["price"]), "size": float(level["longCountPercent"])} 
                                for level in data["orderBook"]["buckets"][:depth]],
                        "asks": [{"price": float(level["price"]), "size": float(level["shortCountPercent"])} 
                                for level in data["orderBook"]["buckets"][:depth]],
                        "time": data["orderBook"]["time"]
                    }
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get orderbook for {symbol}: {error_msg}")
                    return {"symbol": symbol, "error": error_msg}
        except Exception as e:
            self.logger.exception(f"Error fetching orderbook for {symbol}: {str(e)}")
            return {"symbol": symbol, "error": str(e)}
    
    async def get_candles(
        self, symbol: str, timeframe: str, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical candles"""
        # Convert our standard timeframe to Oanda format
        timeframe_map = {
            "1m": "M1", "5m": "M5", "15m": "M15", "30m": "M30",
            "1h": "H1", "4h": "H4", "1d": "D", "1w": "W"
        }
        oanda_timeframe = timeframe_map.get(timeframe, "H1")
        
        endpoint = f"{self.api_url}/instruments/{symbol}/candles"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        params = {
            "price": "M",
            "granularity": oanda_timeframe,
            "count": min(limit, 5000)  # Oanda limit
        }
        
        # Add time parameters if provided
        if start_time:
            params["from"] = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        if end_time:
            params["to"] = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        try:
            async with self._rate_limited_request(
                self.session.get(endpoint, headers=headers, params=params)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    candles = []
                    
                    for candle in data["candles"]:
                        if candle["complete"]:  # Only include complete candles
                            candles.append({
                                "time": candle["time"],
                                "open": float(candle["mid"]["o"]),
                                "high": float(candle["mid"]["h"]),
                                "low": float(candle["mid"]["l"]),
                                "close": float(candle["mid"]["c"]),
                                "volume": float(candle["volume"])
                            })
                    
                    return candles
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get candles for {symbol}: {error_msg}")
                    return []
        except Exception as e:
            self.logger.exception(f"Error fetching candles for {symbol}: {str(e)}")
            return []
    
    async def place_order(self, order: Order) -> Order:
        """Place an order with Oanda"""
        endpoint = f"{self.api_url}/accounts/{self.account_id}/orders"
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
        # Map from our standard order model to Oanda's expected format
        order_request = {
            "order": {
                "units": str(int(order.quantity) if order.side == OrderSide.BUY else -int(order.quantity)),
                "instrument": order.symbol,
                "timeInForce": "GTC",
            }
        }
        
        # Set order type-specific fields
        if order.order_type == OrderType.MARKET:
            order_request["order"]["type"] = "MARKET"
        elif order.order_type == OrderType.LIMIT:
            order_request["order"]["type"] = "LIMIT"
            order_request["order"]["price"] = str(order.price)
        elif order.order_type == OrderType.STOP:
            order_request["order"]["type"] = "STOP"
            order_request["order"]["price"] = str(order.stop_price)
        
        try:
            async with self._rate_limited_request(
                self.session.post(endpoint, headers=headers, json=order_request)
            ) as response:
                data = await response.json()
                
                if response.status == 201:  # Created
                    # Extract order details from response
                    order_details = data.get("orderCreateTransaction", {})
                    
                    # Update our order model with exchange data
                    order.exchange_order_id = order_details.get("id")
                    order.status = OrderStatus.OPEN
                    order.created_at = datetime.now()
                    
                    self.logger.info(f"Order placed successfully: {order.exchange_order_id}")
                    return order
                else:
                    error_message = data.get("errorMessage", "Unknown error")
                    self.logger.error(f"Failed to place order: {error_message}")
                    order.status = OrderStatus.REJECTED
                    return order
        except Exception as e:
            self.logger.exception(f"Error placing order: {str(e)}")
            order.status = OrderStatus.REJECTED
            return order
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an existing order"""
        endpoint = f"{self.api_url}/accounts/{self.account_id}/orders/{order_id}/cancel"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        
        try:
            async with self._rate_limited_request(
                self.session.put(endpoint, headers=headers)
            ) as response:
                if response.status == 200:
                    self.logger.info(f"Order {order_id} canceled successfully")
                    return True
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to cancel order {order_id}: {error_msg}")
                    return False
        except Exception as e:
            self.logger.exception(f"Error canceling order {order_id}: {str(e)}")
            return False
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        endpoint = f"{self.api_url}/accounts/{self.account_id}/pendingOrders"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        
        try:
            async with self._rate_limited_request(
                self.session.get(endpoint, headers=headers)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    orders = []
                    
                    for order_data in data["orders"]:
                        # Filter by symbol if provided
                        if symbol and order_data["instrument"] != symbol:
                            continue
                        
                        # Map Oanda order to our standard Order model
                        order_type = OrderType.MARKET
                        if order_data["type"] == "LIMIT":
                            order_type = OrderType.LIMIT
                        elif order_data["type"] == "STOP":
                            order_type = OrderType.STOP
                        
                        units = int(order_data["units"])
                        side = OrderSide.BUY if units > 0 else OrderSide.SELL
                        quantity = abs(units)
                        
                        order = Order(
                            symbol=order_data["instrument"],
                            order_type=order_type,
                            side=side,
                            quantity=quantity,
                            price=float(order_data.get("price", 0)),
                            stop_price=float(order_data.get("price", 0)) if order_type == OrderType.STOP else None,
                            exchange_order_id=order_data["id"],
                            status=OrderStatus.OPEN,
                            created_at=datetime.fromisoformat(order_data["createTime"].replace("Z", "+00:00"))
                        )
                        orders.append(order)
                    
                    return orders
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get open orders: {error_msg}")
                    return []
        except Exception as e:
            self.logger.exception(f"Error fetching open orders: {str(e)}")
            return []
    
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        endpoint = f"{self.api_url}/accounts/{self.account_id}/openPositions"
        headers = {"Authorization": f"Bearer {self.api_token}"}
        
        try:
            async with self._rate_limited_request(
                self.session.get(endpoint, headers=headers)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    positions = []
                    
                    for position_data in data["positions"]:
                        instrument = position_data["instrument"]
                        long_units = int(position_data["long"]["units"])
                        short_units = int(position_data["short"]["units"])
                        
                        # Determine overall position direction
                        if long_units > 0:
                            direction = "buy"
                            units = long_units
                            avg_price = float(position_data["long"]["averagePrice"])
                            pl = float(position_data["long"]["unrealizedPL"])
                        else:
                            direction = "sell"
                            units = abs(short_units)
                            avg_price = float(position_data["short"]["averagePrice"])
                            pl = float(position_data["short"]["unrealizedPL"])
                        
                        position = {
                            "symbol": instrument,
                            "direction": direction,
                            "quantity": units,
                            "entry_price": avg_price,
                            "unrealized_pnl": pl,
                            "raw_data": position_data
                        }
                        positions.append(position)
                    
                    return positions
                else:
                    error_msg = await response.text()
                    self.logger.error(f"Failed to get positions: {error_msg}")
                    return []
        except Exception as e:
            self.logger.exception(f"Error fetching positions: {str(e)}")
            return []
    
    async def close_position(self, symbol: str, amount: Optional[float] = None) -> bool:
        """Close a position for a symbol"""
        # If amount is None, close the entire position
        if amount is None:
            endpoint = f"{self.api_url}/accounts/{self.account_id}/positions/{symbol}/close"
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            }
            
            # Close all units
            data = {"longUnits": "ALL", "shortUnits": "ALL"}
            
            try:
                async with self._rate_limited_request(
                    self.session.put(endpoint, headers=headers, json=data)
                ) as response:
                    if response.status == 200:
                        self.logger.info(f"Position {symbol} closed successfully")
                        return True
                    else:
                        error_msg = await response.text()
                        self.logger.error(f"Failed to close position {symbol}: {error_msg}")
                        return False
            except Exception as e:
                self.logger.exception(f"Error closing position {symbol}: {str(e)}")
                return False
        
        # For partial closes, create a new order in the opposite direction
        else:
            # First, get position details to determine direction
            positions = await self.get_positions()
            position = next((p for p in positions if p["symbol"] == symbol), None)
            
            if not position:
                self.logger.error(f"No open position found for {symbol}")
                return False
            
            # Create an order in the opposite direction
            close_side = OrderSide.SELL if position["direction"] == "buy" else OrderSide.BUY
            
            # Limit amount to the actual position size
            close_amount = min(amount, position["quantity"])
            
            order = Order(
                symbol=symbol,
                order_type=OrderType.MARKET,
                side=close_side,
                quantity=close_amount
            )
            
            result = await self.place_order(order)
            return result.status != OrderStatus.REJECTED


class BinanceAdapter(ExchangeAdapter):
    """Binance exchange adapter implementation"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_url = config.get("api_url", "https://api.binance.com")
        self.api_key = config.get("api_key")
        self.api_secret = config.get("api_secret")
        
        if not self.api_key or not self.api_secret:
            raise ValueError("Binance adapter requires api_key and api_secret in config")
        
        # Setup rate limits specific to Binance
        self.rate_limit_semaphore = asyncio.Semaphore(
            config.get("rate_limit_max_concurrent", 10)
        )
    
    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """Generate HMAC-SHA256 signature for API request"""
        query_string = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    async def _signed_request(self, method: str, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a signed request to the Binance API"""
        if params is None:
            params = {}
        
        # Add timestamp for signature
        params['timestamp'] = int(time_module.time() * 1000)
        
        # Generate signature
        signature = self._generate_signature(params)
        params['signature'] = signature
        
        # Prepare URL and headers
        url = f"{self.api_url}{endpoint}"
        headers = {
            'X-MBX-APIKEY': self.api_key
        }
        
        # Make the request
        try:
            if method.lower() == 'get':
                async with self._rate_limited_request(
                    self.session.get(url, headers=headers, params=params)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = await response.text()
                        self.logger.error(f"Binance API error: {error_msg}")
                        return {'error': error_msg, 'status_code': response.status}
            elif method.lower() == 'post':
                async with self._rate_limited_request(
                    self.session.post(url, headers=headers, params=params)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = await response.text()
                        self.logger.error(f"Binance API error: {error_msg}")
                        return {'error': error_msg, 'status_code': response.status}
            elif method.lower() == 'delete':
                async with self._rate_limited_request(
                    self.session.delete(url, headers=headers, params=params)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = await response.text()
                        self.logger.error(f"Binance API error: {error_msg}")
                        return {'error': error_msg, 'status_code': response.status}
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
        except Exception as e:
            self.logger.exception(f"Error in Binance API request: {str(e)}")
            return {'error': str(e)}
    
    async def _public_request(self, method: str, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make a public request to the Binance API"""
        if params is None:
            params = {}
        
        # Prepare URL
        url = f"{self.api_url}{endpoint}"
        
        # Make the request
        try:
            if method.lower() == 'get':
                async with self._rate_limited_request(
                    self.session.get(url, params=params)
                ) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        error_msg = await response.text()
                        self.logger.error(f"Binance API error: {error_msg}")
                        return {'error': error_msg, 'status_code': response.status}
            else:
                raise ValueError(f"Unsupported HTTP method for public request: {method}")
        except Exception as e:
            self.logger.exception(f"Error in Binance API request: {str(e)}")
            return {'error': str(e)}
    
    async def get_account_info(self) -> Dict[str, Any]:
        """Get Binance account information"""
        endpoint = "/api/v3/account"
        
        try:
            data = await self._signed_request('get', endpoint)
            
            if 'error' in data:
                return data
            
            # Transform to our standardized format
            return {
                "balance": sum(float(asset['free']) for asset in data['balances'] if float(asset['free']) > 0),
                "currency": "USDT",  # Default currency for Binance
                "margin_available": float(next((asset['free'] for asset in data['balances'] if asset['asset'] == 'USDT'), 0)),
                "open_position_count": len([asset for asset in data['balances'] if float(asset['locked']) > 0]),
                "unrealized_pl": 0.0,  # Binance doesn't provide this directly
                "balances": {asset['asset']: float(asset['free']) for asset in data['balances'] if float(asset['free']) > 0}
            }
        except Exception as e:
            self.logger.exception(f"Error fetching account info: {str(e)}")
            return {"error": str(e)}
    
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """Get current ticker for a symbol from Binance"""
        endpoint = "/api/v3/ticker/price"
        params = {"symbol": symbol}
        
        try:
            data = await self._public_request('get', endpoint, params)
            
            if 'error' in data:
                return {"symbol": symbol, "error": data['error']}
            
            # Get additional 24h data for more information
            book_ticker = await self._public_request('get', "/api/v3/ticker/bookTicker", params)
            
            if 'error' in book_ticker:
                # Still return basic price info even if book ticker fails
                return {
                    "symbol": symbol,
                    "price": float(data['price']),
                    "time": datetime.now().isoformat()
                }
            
            return {
                "symbol": symbol,
                "bid": float(book_ticker['bidPrice']),
                "ask": float(book_ticker['askPrice']),
                "price": float(data['price']),
                "time": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.exception(f"Error fetching ticker for {symbol}: {str(e)}")
            return {"symbol": symbol, "error": str(e)}
    
    async def get_orderbook(self, symbol: str, depth: int = 10) -> Dict[str, Any]:
        """Get order book for a symbol from Binance"""
        endpoint = "/api/v3/depth"
        params = {
            "symbol": symbol,
            "limit": min(depth, 1000)  # Binance max limit
        }
        
        try:
            data = await self._public_request('get', endpoint, params)
            
            if 'error' in data:
                return {"symbol": symbol, "error": data['error']}
            
            return {
                "symbol": symbol,
                "bids": [{"price": float(price), "size": float(quantity)} for price, quantity in data['bids'][:depth]],
                "asks": [{"price": float(price), "size": float(quantity)} for price, quantity in data['asks'][:depth]],
                "time": datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.exception(f"Error fetching orderbook for {symbol}: {str(e)}")
            return {"symbol": symbol, "error": str(e)}
    
    async def get_candles(
        self, symbol: str, timeframe: str, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical candles from Binance"""
        # Convert our standard timeframe to Binance format
        timeframe_map = {
            "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
            "1h": "1h", "4h": "4h", "1d": "1d", "1w": "1w"
        }
        binance_timeframe = timeframe_map.get(timeframe, "1h")
        
        endpoint = "/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": binance_timeframe,
            "limit": min(limit, 1000)  # Binance max limit
        }
        
        # Add time parameters if provided
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)
        
        try:
            data = await self._public_request('get', endpoint, params)
            
            if 'error' in data:
                return []
            
            candles = []
            
            # Binance klines format:
            # [
            #   [
            #     1499040000000,      // Open time
            #     "0.01634790",       // Open
            #     "0.80000000",       // High
            #     "0.01575800",       // Low
            #     "0.01577100",       // Close
            #     "148976.11427815",  // Volume
            #     1499644799999,      // Close time
            #     "2434.19055334",    // Quote asset volume
            #     308,                // Number of trades
            #     "1756.87402397",    // Taker buy base asset volume
            #     "28.46694368",      // Taker buy quote asset volume
            #     "17928899.62484339" // Ignore
            #   ]
            # ]
            for kline in data:
                candles.append({
                    "time": datetime.fromtimestamp(kline[0] / 1000).isoformat(),
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5])
                })
            
            return candles
        except Exception as e:
            self.logger.exception(f"Error fetching candles for {symbol}: {str(e)}")
            return []
    
    async def place_order(self, order: Order) -> Order:
        """Place an order with Binance"""
        endpoint = "/api/v3/order"
        
        # Map our order model to Binance's expected format
        params = {
            "symbol": order.symbol,
            "side": "BUY" if order.side == OrderSide.BUY else "SELL",
            "quantity": order.quantity
        }
        
        # Set order type-specific parameters
        if order.order_type == OrderType.MARKET:
            params["type"] = "MARKET"
        elif order.order_type == OrderType.LIMIT:
            if not order.price:
                order.status = OrderStatus.REJECTED
                return order
            params["type"] = "LIMIT"
            params["price"] = order.price
            params["timeInForce"] = order.time_in_force if order.time_in_force else "GTC"
        elif order.order_type == OrderType.STOP:
            if not order.stop_price:
                order.status = OrderStatus.REJECTED
                return order
            params["type"] = "STOP_LOSS"
            params["stopPrice"] = order.stop_price
            params["timeInForce"] = order.time_in_force if order.time_in_force else "GTC"
        
        # Add custom client order ID if provided
        if order.client_order_id:
            params["newClientOrderId"] = order.client_order_id
        
        try:
            data = await self._signed_request('post', endpoint, params)
            
            if 'error' in data or 'code' in data:
                self.logger.error(f"Failed to place order: {data.get('error', data.get('msg', 'Unknown error'))}")
                order.status = OrderStatus.REJECTED
                return order
            
            # Update order with response data
            order.exchange_order_id = str(data.get('orderId'))
            
            if data.get('status') == 'FILLED':
                order.status = OrderStatus.FILLED
            elif data.get('status') == 'PARTIALLY_FILLED':
                order.status = OrderStatus.PARTIALLY_FILLED
            else:
                order.status = OrderStatus.OPEN
                
            order.created_at = datetime.fromtimestamp(data.get('transactTime', time_module.time() * 1000) / 1000)
            
            self.logger.info(f"Order placed successfully: {order.exchange_order_id}")
            return order
        except Exception as e:
            self.logger.exception(f"Error placing order: {str(e)}")
            order.status = OrderStatus.REJECTED
            return order
    
    async def cancel_order(self, order_id: str, symbol: str) -> bool:
        """Cancel an existing order"""
        endpoint = "/api/v3/order"
        params = {
            "symbol": symbol,
            "orderId": order_id
        }
        
        try:
            data = await self._signed_request('delete', endpoint, params)
            
            if 'error' in data or 'code' in data:
                self.logger.error(f"Failed to cancel order {order_id}: {data.get('error', data.get('msg', 'Unknown error'))}")
                return False
            
            self.logger.info(f"Order {order_id} canceled successfully")
            return True
        except Exception as e:
            self.logger.exception(f"Error canceling order {order_id}: {str(e)}")
            return False
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get all open orders"""
        endpoint = "/api/v3/openOrders"
        params = {}
        
        if symbol:
            params["symbol"] = symbol
        
        try:
            data = await self._signed_request('get', endpoint, params)
            
            if 'error' in data:
                self.logger.error(f"Failed to get open orders: {data.get('error')}")
                return []
            
            orders = []
            
            for order_data in data:
                # Map Binance order to our standard Order model
                order_type = OrderType.MARKET
                if order_data['type'] == 'LIMIT':
                    order_type = OrderType.LIMIT
                elif 'STOP' in order_data['type']:
                    order_type = OrderType.STOP
                
                side = OrderSide.BUY if order_data['side'] == 'BUY' else OrderSide.SELL
                
                order = Order(
                    symbol=order_data['symbol'],
                    order_type=order_type,
                    side=side,
                    quantity=float(order_data['origQty']),
                    price=float(order_data.get('price', 0)),
                    stop_price=float(order_data.get('stopPrice', 0)) if 'stopPrice' in order_data else None,
                    exchange_order_id=str(order_data['orderId']),
                    client_order_id=order_data.get('clientOrderId'),
                    status=OrderStatus.OPEN,
                    created_at=datetime.fromtimestamp(order_data['time'] / 1000)
                )
                orders.append(order)
            
            return orders
        except Exception as e:
            self.logger.exception(f"Error fetching open orders: {str(e)}")
            return []
    
    async def get_positions(self) -> List[Dict[str, Any]]:
        """Get all open positions"""
        # For spot trading in Binance, positions are just non-zero balances
        # For futures, we would need to use a different endpoint
        endpoint = "/api/v3/account"
        
        try:
            data = await self._signed_request('get', endpoint)
            
            if 'error' in data:
                self.logger.error(f"Failed to get positions: {data.get('error')}")
                return []
            
            positions = []
            
            # Get current prices for all assets with non-zero balances
            non_zero_balances = [asset for asset in data['balances'] 
                               if float(asset['free']) > 0 or float(asset['locked']) > 0]
            
            if not non_zero_balances:
                return []
            
            for asset in non_zero_balances:
                symbol = f"{asset['asset']}USDT"
                quantity = float(asset['free']) + float(asset['locked'])
                
                if quantity <= 0:
                    continue
                
                # Try to get current price
                try:
                    ticker = await self.get_ticker(symbol)
                    price = ticker.get('price', 0)
                except:
                    price = 0
                
                position = {
                    "symbol": symbol,
                    "direction": "buy",  # Spot positions are always "long"
                    "quantity": quantity,
                    "entry_price": 0,  # Not available from Binance API for spot
                    "current_price": price,
                    "unrealized_pnl": 0,  # Not available without historical data
                    "raw_data": asset
                }
                positions.append(position)
            
            return positions
        except Exception as e:
            self.logger.exception(f"Error fetching positions: {str(e)}")
            return []
    
    async def close_position(self, symbol: str, amount: Optional[float] = None) -> bool:
        """Close a position for a symbol"""
        # For Binance spot, closing a position means selling the asset
        
        # First, check if we have the asset
        account_info = await self.get_account_info()
        
        if 'error' in account_info:
            return False
        
        # Extract base asset from symbol (e.g., "BTCUSDT" -> "BTC")
        base_asset = symbol[:-4] if symbol.endswith("USDT") else symbol[:-3]
        
        # Check if we have any balance
        balances = account_info.get('balances', {})
        balance = next((balances.get(base_asset, 0)), 0)
        
        if balance <= 0:
            self.logger.error(f"No balance found for {base_asset}")
            return False
        
        # Determine amount to sell
        sell_amount = min(balance, amount) if amount else balance
        
        # Create market sell order
        order = Order(
            symbol=symbol,
            order_type=OrderType.MARKET,
            side=OrderSide.SELL,
            quantity=sell_amount
        )
        
        result = await self.place_order(order)
        return result.status != OrderStatus.REJECTED


class ExchangeAdapterFactory:
    """Factory for creating exchange adapters"""
    
    @staticmethod
    async def create_adapter(exchange_id: str, config: Dict[str, Any]) -> ExchangeAdapter:
        """
        Create and initialize an exchange adapter
        
        Args:
            exchange_id: Identifier for the exchange (e.g., 'oanda', 'ib')
            config: Exchange-specific configuration
            
        Returns:
            Initialized exchange adapter
            
        Raises:
            ValueError: If exchange_id is not supported
        """
        if exchange_id.lower() == "oanda":
            adapter = OandaAdapter(config)
        elif exchange_id.lower() == "binance":
            adapter = BinanceAdapter(config)
        # Add other exchanges as needed
        # elif exchange_id.lower() == "interactive_brokers":
        #     adapter = IBAdapter(config)
        else:
            raise ValueError(f"Unsupported exchange: {exchange_id}")
        
        # Initialize the adapter
        await adapter.initialize()
        return adapter 