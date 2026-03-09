"""
Pydantic models for the Rozetka Seller API responses.
Based on: https://api.seller.rozetka.com.ua/ (v0.0.1)
"""
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, field_validator


class RozetkaAuthResponse(BaseModel):
    """Response from POST /sites (login)."""
    id: int
    access_token: str


class RozetkaCity(BaseModel):
    '''City information in Rozetka delivery details.'''
    id: Optional[int] = None
    name: Optional[str] = None


class RozetkaDelivery(BaseModel):
    '''Delivery details in Rozetka order.'''
    delivery_service_id: Optional[int] = None
    delivery_service_name: Optional[str] = None
    recipient_title: Optional[str] = None
    delivery_method_id: Optional[int] = None
    cost: Optional[Decimal] = None
    city: Optional[RozetkaCity] = None

    @field_validator("cost", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, handling None.'''
        return Decimal(str(v)) if v is not None else None


class RozetkaOrderItem(BaseModel):
    """A single purchase line inside a Rozetka order (from purchases[] expand)."""
    id: int                           # purchase_id
    item_id: Optional[int] = None     # Rozetka product ID
    item_name: Optional[str] = None
    quantity: int = 1
    price: Optional[Decimal] = None   # price per unit
    cost: Optional[Decimal] = None    # total line (price × qty)
    cost_with_discount: Optional[Decimal] = None

    @field_validator("price", "cost", "cost_with_discount", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, handling None.'''
        return Decimal(str(v)) if v is not None else None


class RozetkaOrder(BaseModel):
    """
    Rozetka order from GET /orders/search
    with expand=purchases,user,delivery
    """
    id: int                            # rozetka_order_id
    market_id: Optional[int] = None
    created: Optional[datetime] = None
    changed: Optional[datetime] = None
    amount: Optional[Decimal] = None
    amount_with_discount: Optional[Decimal] = None
    cost: Optional[Decimal] = None
    cost_with_discount: Optional[Decimal] = None
    status: Optional[int] = None
    status_group: Optional[int] = None   # 1=Processing, 2=Successful, 3=Unsuccessful
    user_phone: Optional[str] = None
    ttn: Optional[str] = None           # waybill / tracking number
    total_quantity: Optional[int] = None
    delivery: Optional[RozetkaDelivery] = None
    purchases: list[RozetkaOrderItem] = []

    @field_validator("amount", "amount_with_discount", "cost", "cost_with_discount", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, handling None.'''
        return Decimal(str(v)) if v is not None else None


class RozetkaOrdersPage(BaseModel):
    """Wrapper for paginated /orders/search response."""
    orders: list[RozetkaOrder] = []
    total_count: int = 0
    page_count: int = 1
    current_page: int = 1
    per_page: int = 20

