from typing import Any, List, Tuple, Type, Optional, Dict
from pydantic import BaseModel, Field, ValidationError, field_validator, validator
from datetime import datetime
from loguru import logger


class ShipbobOrderDetails(BaseModel):
  created_date: datetime
  purchase_date: datetime
  shipbob_order_id: int
  order_number: str
  order_status: str
  order_type: str
  channel_id: int
  channel_name: str
  product_id: int
  sku: str
  shipping_method: str
  customer_name: Optional[str]
  customer_email: Optional[str]
  customer_address_city: Optional[str]
  customer_address_state: Optional[str]
  customer_address_country: Optional[str]
  sku_name: str
  inventory_id: int
  inventory_name: str
  inventory_qty: int

  @field_validator('created_date', 'purchase_date')
  @classmethod
  def validate_timestamps(cls, value):
    if isinstance(value, datetime):
      # Format timestamp
      return value.strftime('%Y-%m-%dT%H:%M:%S')
    elif isinstance(value, str):
      try:
        # Attempt to parse the string to a datetime object
        return pd.to_datetime(value).strftime('%Y-%m-%dT%H:%M:%S')
      except ValueError:
        raise ValueError(
            f"{cls} must be in ISO 8601 format if provided as a string")

    raise ValueError(
        f"{cls} must be a datetime object or a string in ISO 8601 format")


class ShipbobInventory(BaseModel):
  id: int
  name: str
  is_digital: bool
  is_case_pick: bool
  is_lot: bool
  total_fulfillable_quantity: int
  total_onhand_quantity: int
  total_committed_quantity: int
  total_sellable_quantity: int
  total_awaiting_quantity: int
  total_exception_quantity: int
  total_internal_transfer_quantity: int
  total_backordered_quantity: int
  is_active: bool


class DailyRunRate(BaseModel):
  inventory_id: int
  run_rate: float
  name: str
  total_fulfillable_quantity: int
  est_stock_days_on_hand: float
  estimated_stockout_date: datetime
  restock_point: int

class ShopifyProductVariantDetails(BaseModel):
  product_id: int
  product_title: str
  variant_id: int
  variant_title: str
  variant_sku: str
  inventory_quantity: float
  published_at: datetime

  @validator('inventory_quantity')
  def quantity_must_be_non_negative(cls, value):
      assert value >= 0, 'inventory_quantity must be non-negative'
      return value