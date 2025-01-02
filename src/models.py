from typing import Any, List, Tuple, Type, Optional, Dict
from pydantic import BaseModel, Field, ValidationError, field_validator, validator, root_validator, model_validator, EmailStr
from datetime import datetime, date
from loguru import logger
import math
import re


class ShopifyLineItem(BaseModel):
    order_id: int = Field(..., description="Unique identifier for the order")
    email: EmailStr = Field(..., description="Customer email address")
    created_at: datetime = Field(
        ..., description="Timestamp when the order was created in ISO format")
    order_date: datetime = Field(
        ..., description="Date when the order was placed in ISO format")
    price: float = Field(..., ge=0, description="Price of the line item")
    quantity: int = Field(..., ge=0, description="Quantity of the line item")
    sku: str = Field(
        ..., description="Stock keeping unit identifier for the product")
    title: str = Field(..., description="Title of the product")
    variant_title: Optional[str] = Field(
        None, description="Title of the product variant")
    line_item_name: str = Field(..., description="Name of the line item")

    @field_validator('*', mode='before')
    @classmethod
    def replace_delimiter(cls, value):
        # Delimiter to check for
        delimiter = ','
        replacement = ' '  # Replace delimiter with another safe character

        # Apply replacement only for string fields
        if isinstance(value, str) and delimiter in value:
            return value.replace(delimiter, replacement)
        return value

    @field_validator('email', mode='before')
    @classmethod
    def validate_email(cls, value):
        # Replace blank emails or invalid emails with a default test email
        try:
            # Attempt to validate email using EmailStr
            return EmailStr._validate(value)
        except (ValidationError, ValueError):
            return 'unknown@unknown.com'

    @field_validator('created_at', 'order_date', mode='before')
    @classmethod
    def validate_datetime(cls, value):
        # Coerce datetime fields to a timestamp format compatible with Glue table timestamp data type
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError(f"Invalid datetime format for value: {value}")
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return value

    @field_validator('sku', mode='before')
    @classmethod
    def validate_nullable_string(cls, value):
        # Replace NaN values with None for string fields
        if isinstance(value, float) and math.isnan(value) or value=='' or not value:
            return 'No SKU'
        return value



class ShopifyOrder(BaseModel):
    order_id: int = Field(..., description="Unique identifier for the order")
    email: EmailStr = Field(..., description="Customer email address")
    created_at: datetime = Field(
        ..., description="Timestamp when the order was created in ISO format")
    shipping_address: Optional[str] = Field(...,
                                  description="Shipping address for the order")
    shipping_city: Optional[str] = Field(..., description="City of the shipping address")
    shipping_province: Optional[str] = Field(
        ..., description="Province or state of the shipping address")
    shipping_country: Optional[str] = Field(
        ..., description="Country of the shipping address")
    subtotal_price: float = Field(...,
                                  ge=0,
                                  description="Subtotal price of the order")
    total_line_items_price: float = Field(
        ..., ge=0, description="Total price of line items in the order")
    total_tax: float = Field(...,
                             ge=0,
                             description="Total tax applied to the order")
    total_discounts: float = Field(
        ..., ge=0, description="Total discounts applied to the order")
    total_shipping_fee: float = Field(
        ..., ge=0, description="Total shipping fee for the order")
    total_price: float = Field(...,
                               ge=0,
                               description="Final total price for the order")
    order_date: datetime = Field(
        ..., description="Date when the order was placed in ISO format")

    @field_validator('*', mode='before')
    @classmethod
    def replace_delimiter(cls, value):
        # Delimeter to check for
        delimiter = ','
        replacement = ' '  # Replace delimeter another safe character

        # Apply replacement only for string fields
        if isinstance(value, str) and delimiter in value:
            return value.replace(delimiter, replacement)
        return value

    @field_validator('email', mode='before')
    @classmethod
    def validate_email(cls, value):
        # Replace blank emails or invalid emails with a default test email
        try:
            # Attempt to validate email using EmailStr
            return EmailStr._validate(value)
        except (ValidationError, ValueError):
            return 'unknown@unknown.com'

    @field_validator('created_at', 'order_date', mode='before')
    @classmethod
    def validate_datetime(cls, value):
        # Coerce datetime fields to a timestamp format compatible with Glue table timestamp data type
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise ValueError(f"Invalid datetime format for value: {value}")
        elif isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        return value

    @field_validator('shipping_address', 'shipping_city', 'shipping_province', 'shipping_country', 'sku', mode='before',check_fields=False)
    @classmethod
    def validate_nullable_string(cls, value):
        # Replace NaN values with None for string fields
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
        
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

    @field_validator('*', mode='before')
    @classmethod
    def replace_delimiter(cls, value):
        # Delimeter to check for
        delimiter = ','
        replacement = ' '  # Replace delimeter another safe character

        # Apply replacement only for string fields
        if isinstance(value, str) and delimiter in value:
            return value.replace(delimiter, replacement)
        return value

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
                    f"{cls} must be in ISO 8601 format if provided as a string"
                )

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


class KatanaInventory(BaseModel):
    name: str
    variant_code_sku: Optional[str]
    category: Optional[str]
    default_supplier: Optional[str]
    units_of_measure: str
    average_cost: float
    value_in_stock: float
    in_stock: float
    expected: float
    committed: float
    safety_stock: float
    calculated_stock: float
    location: str

    # Custom field validator to handle NaN values
    @field_validator('category',
                     'default_supplier',
                     'variant_code_sku',
                     mode='before')
    def handle_nan(cls, v):
        if isinstance(v, float) and math.isnan(v):
            return None
        return v


class KatanaRecipeIngredient(BaseModel):
    product_variant_code: Optional[float] = None
    product_variant_name: Optional[str] = None
    product_supplier_item_code: Optional[str] = None
    product_internal_barcode: Optional[str] = None
    product_registered_barcode: Optional[str] = None
    ingredient_variant_code_sku_required: str
    ingredient_variant_name: Optional[str] = None
    ingredient_supplier_item_code: Optional[str] = None
    ingredient_internal_barcode: Optional[str] = None
    ingredient_registered_barcode: Optional[str] = None
    notes: Optional[str] = None
    quantity_required: float
    unit_of_measure: Optional[str] = None
    current_stock_price: float

    @root_validator(pre=True)
    def replace_nan_with_none(cls, values):
        for field, value in values.items():
            if isinstance(value, float) and math.isnan(value):
                values[field] = None
        return values

    @validator('product_variant_code',
               'quantity_required',
               'current_stock_price',
               pre=True,
               allow_reuse=True)
    def validate_float(cls, v):
        if v is None:
            return v
        try:
            return float(v)
        except ValueError:
            raise ValueError(f"Invalid float value: {v}")


class RawMaterialRunRate(BaseModel):
    katana_ingredient_sku: str
    katana_ingredient_name: str
    katana_unit_of_measure: str
    daily_run_rate: float
    inventory_on_hand: float
    inventory_as_of: date
    days_on_hand: float
    reorder_point: Optional[float]


class ManufacturingOrder(BaseModel):
    mo: str
    created_date: datetime
    done_date: Optional[datetime] = None
    production_status: str
    product_variant_code_sku: float
    product_variant: str
    planned_quantity_of_product: float
    actual_quantity_of_product: Optional[float] = None
    unit_of_measure: str
    ingredient_variant_code_sku: str
    ingredient_variant: str
    ingredient_notes: Optional[str] = None
    planned_quantity_of_ingredient: float
    actual_quantity_of_ingredient: Optional[float] = None
    ingredient_unit_of_measure: str
    ingredient_cost: float
    ingredient_status: str

    @model_validator(mode='before')
    def replace_nan_with_none(cls, values):
        for field, value in values.items():
            if isinstance(value, float) and math.isnan(value):
                values[field] = None
        return values

    @field_validator('created_date', 'done_date', mode='before')
    def parse_dates(cls, value):
        if value is None or value == '':
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            raise ValueError(
                f"Invalid date format. Expected YYYY-MM-DD, got {value}")


            
class RawMaterialStatus(BaseModel):
    name: str
    units_of_measure: str
    in_stock: float
    in_stock_as_of: date
    planned_qty: float
    planned_qty_as_of: date
    inventory_remaining: float
    in_stock_percentage: float
    needs_replenished: bool
