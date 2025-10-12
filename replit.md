# ShipBob Product Tracker

## Overview
This project helps you track unique products from ShipBob using a PostgreSQL database and a Streamlit web interface.

## Recent Changes (October 12, 2025)
- Set up PostgreSQL database for tracking ShipBob products
- Created `shipbob_products` table with the following fields:
  - `id`: Auto-incrementing primary key
  - `inventory_id`: Unique ShipBob inventory ID
  - `product_name`: Name of the product
  - `size_variant`: Size variant (e.g., Large Bag, Bulk Bag, Single Serve)
  - `sku_variant`: SKU/Flavor variant
  - `product_type_variant`: Product type (e.g., Creamer, Coffee, Merch)
  - `limited_edition_flag`: Whether the product is limited edition
  - `seasonal_flag`: Whether the product is seasonal
  - `created_at` & `updated_at`: Timestamps
- Created Streamlit web app (`app.py`) for managing products

## Project Structure
- `app.py` - Main Streamlit application for tracking ShipBob products
- `src/` - Contains various data pipeline modules for ShipBob, Shopify, Katana integrations
- `src/utils.py` - Utility functions including ShipBob API integration
- `src/models.py` - Pydantic models for data validation
- `requirements.txt` - Python dependencies

## Database Access
The PostgreSQL database is accessible via the `DATABASE_URL` environment variable (already configured).

## How to Use
The Streamlit app is running on port 5000 and provides three main features:

1. **View Products**: See all your tracked ShipBob products in a table
2. **Add Product**: Add new unique products with their metadata
3. **Update Product**: Modify existing product information

## Existing Integrations
The project includes data pipeline integrations for:
- ShipBob (inventory, orders, product metadata)
- Shopify (orders, product variants)
- Katana (inventory, recipes, manufacturing orders)
- Raw material tracking and run rates

## Notes
- The database is ready to track your unique ShipBob products
- You can manually add products through the web interface
- ShipBob API functions are available in `src/utils.py` if you want to automate data fetching
