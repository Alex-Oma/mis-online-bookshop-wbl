"""
Pydantic models that mirror the OpenCart MySQL source tables.
Used to validate rows extracted from the OpenCart database
before they are written to staging tables.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, field_validator


class OcOrder(BaseModel):
    """Mirrors oc_order table in OpenCart DB."""
    order_id: int
    store_id: int = 0
    customer_id: int = 0
    customer_group_id: int = 0
    payment_city: Optional[str] = None
    payment_country: Optional[str] = None
    shipping_city: Optional[str] = None
    shipping_method: Optional[str] = None
    payment_method: Optional[str] = None
    total: Decimal = Decimal("0.0000")
    order_status_id: int = 0
    currency_code: str = "UAH"
    currency_value: Decimal = Decimal("1.00000000")
    date_added: datetime
    date_modified: datetime

    @field_validator("total", "currency_value", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, treating None as 0.'''
        return Decimal(str(v)) if v is not None else Decimal("0")


class OcOrderProduct(BaseModel):
    """Mirrors oc_order_product table in OpenCart DB."""
    order_product_id: int
    order_id: int
    product_id: int
    name: str
    model: Optional[str] = None
    quantity: int
    price: Decimal = Decimal("0.0000")
    total: Decimal = Decimal("0.0000")
    tax: Decimal = Decimal("0.0000")

    @field_validator("price", "total", "tax", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, treating None as 0.'''
        return Decimal(str(v)) if v is not None else Decimal("0")


class OcProduct(BaseModel):
    """Mirrors oc_product joined with oc_product_description (language_id=1)."""
    product_id: int
    model: Optional[str] = None
    sku: Optional[str] = None
    isbn: Optional[str] = None
    quantity: int = 0
    stock_status_id: int = 0
    manufacturer_id: int = 0
    price: Decimal = Decimal("0.0000")
    date_available: Optional[date] = None
    publishing_year: Optional[int] = None
    pages_number: Optional[int] = None
    author: Optional[str] = None
    publisher: Optional[str] = None
    binding_type: Optional[str] = None   # pereplet
    status: int = 0
    date_added: Optional[datetime] = None
    date_modified: Optional[datetime] = None
    # From oc_product_description
    name: Optional[str] = None
    description: Optional[str] = None
    # From oc_product_to_category (main_category = 1)
    main_category_id: Optional[int] = None

    @field_validator("price", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        '''Coerce numeric values to Decimal, treating None as 0.'''
        return Decimal(str(v)) if v is not None else Decimal("0")


class OcCategory(BaseModel):
    """Mirrors oc_category joined with oc_category_description (language_id=1)."""
    category_id: int
    parent_id: int = 0
    status: int = 1
    sort_order: int = 0
    # From oc_category_description
    name: Optional[str] = None
    description: Optional[str] = None


class OcManufacturer(BaseModel):
    """Mirrors oc_manufacturer joined with oc_manufacturer_description (language_id=1)."""
    manufacturer_id: int
    name: str
    # From oc_manufacturer_description
    description: Optional[str] = None


class OcCustomer(BaseModel):
    """Mirrors oc_customer — no personal identifiable information to be taken, only metadata kept."""
    customer_id: int
    customer_group_id: int = 0
    store_id: int = 0
    city: Optional[str] = None       # From shipping address (not stored in oc_customer directly)
    country: Optional[str] = None
    date_added: Optional[datetime] = None
    is_newsletter: bool = False
    customer_group_name: Optional[str] = None   # joined from oc_customer_group_description

