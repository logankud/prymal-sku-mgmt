import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime
import os

# Database connection
def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

# Initialize the app
st.set_page_config(page_title="ShipBob Product Tracker", layout="wide")

st.title("🚢 ShipBob Product Tracker")

# Tabs for different operations
tab1, tab2, tab3 = st.tabs(["📊 View Products", "➕ Add Product", "🔄 Update Product"])

# Tab 1: View Products
with tab1:
    st.header("All Products")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT id, inventory_id, product_name, size_variant, sku_variant, 
               product_type_variant, limited_edition_flag, seasonal_flag, 
               created_at, updated_at
        FROM shipbob_products
        ORDER BY created_at DESC
    """)
    
    products = cur.fetchall()
    
    if products:
        df = pd.DataFrame(products, columns=[
            'ID', 'Inventory ID', 'Product Name', 'Size Variant', 'SKU Variant',
            'Product Type', 'Limited Edition', 'Seasonal', 'Created At', 'Updated At'
        ])
        st.dataframe(df, use_container_width=True)
        
        st.metric("Total Products", len(products))
    else:
        st.info("No products found. Add your first product in the 'Add Product' tab!")
    
    cur.close()
    conn.close()

# Tab 2: Add New Product
with tab2:
    st.header("Add New Product")
    
    with st.form("add_product_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            inventory_id = st.number_input("Inventory ID*", min_value=1, step=1)
            product_name = st.text_input("Product Name*")
            size_variant = st.text_input("Size Variant (e.g., Large Bag, Bulk Bag)")
        
        with col2:
            sku_variant = st.text_input("SKU Variant (e.g., Flavor)")
            product_type = st.text_input("Product Type (e.g., Creamer, Coffee, Merch)")
            
        col3, col4 = st.columns(2)
        with col3:
            limited_edition = st.checkbox("Limited Edition")
        with col4:
            seasonal = st.checkbox("Seasonal")
        
        submitted = st.form_submit_button("Add Product")
        
        if submitted:
            if not inventory_id or not product_name:
                st.error("Inventory ID and Product Name are required!")
            else:
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    
                    cur.execute("""
                        INSERT INTO shipbob_products 
                        (inventory_id, product_name, size_variant, sku_variant, 
                         product_type_variant, limited_edition_flag, seasonal_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (inventory_id, product_name, size_variant, sku_variant,
                          product_type, limited_edition, seasonal))
                    
                    conn.commit()
                    cur.close()
                    conn.close()
                    
                    st.success(f"✅ Product '{product_name}' added successfully!")
                    st.rerun()
                    
                except psycopg2.errors.UniqueViolation:
                    st.error(f"❌ A product with Inventory ID {inventory_id} already exists!")
                    conn.rollback()
                except Exception as e:
                    st.error(f"❌ Error adding product: {str(e)}")
                    conn.rollback()

# Tab 3: Update Product
with tab3:
    st.header("Update Product")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("SELECT id, inventory_id, product_name FROM shipbob_products ORDER BY product_name")
    products_list = cur.fetchall()
    
    if products_list:
        product_options = {f"{p[2]} (ID: {p[1]})": p[0] for p in products_list}
        
        selected_product = st.selectbox("Select Product to Update", options=list(product_options.keys()))
        
        if selected_product:
            product_id = product_options[selected_product]
            
            cur.execute("""
                SELECT inventory_id, product_name, size_variant, sku_variant, 
                       product_type_variant, limited_edition_flag, seasonal_flag
                FROM shipbob_products WHERE id = %s
            """, (product_id,))
            
            current = cur.fetchone()
            
            with st.form("update_product_form"):
                col1, col2 = st.columns(2)
                
                with col1:
                    new_product_name = st.text_input("Product Name*", value=current[1])
                    new_size_variant = st.text_input("Size Variant", value=current[2] or "")
                
                with col2:
                    new_sku_variant = st.text_input("SKU Variant", value=current[3] or "")
                    new_product_type = st.text_input("Product Type", value=current[4] or "")
                
                col3, col4 = st.columns(2)
                with col3:
                    new_limited_edition = st.checkbox("Limited Edition", value=current[5])
                with col4:
                    new_seasonal = st.checkbox("Seasonal", value=current[6])
                
                update_submitted = st.form_submit_button("Update Product")
                
                if update_submitted:
                    if not new_product_name:
                        st.error("Product Name is required!")
                    else:
                        try:
                            cur.execute("""
                                UPDATE shipbob_products
                                SET product_name = %s, size_variant = %s, sku_variant = %s,
                                    product_type_variant = %s, limited_edition_flag = %s, 
                                    seasonal_flag = %s, updated_at = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (new_product_name, new_size_variant, new_sku_variant,
                                  new_product_type, new_limited_edition, new_seasonal, product_id))
                            
                            conn.commit()
                            st.success(f"✅ Product '{new_product_name}' updated successfully!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"❌ Error updating product: {str(e)}")
                            conn.rollback()
    else:
        st.info("No products available to update. Add products first!")
    
    cur.close()
    conn.close()

# Footer
st.markdown("---")
st.caption("ShipBob Product Tracker - Manage your unique products")
