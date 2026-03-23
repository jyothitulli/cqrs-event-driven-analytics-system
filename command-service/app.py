"""
Command Service - Handles all write operations (products, orders)
Implements CQRS write side with outbox pattern for reliable event publishing
"""
import os
import json
import uuid
from datetime import datetime
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Database configuration
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://user:password@write-db:5432/write_db')
logger.info(f"Connecting to database: {DATABASE_URL}")

# Create database engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    """Check if service is healthy"""
    try:
        # Test database connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return jsonify({
            'status': 'healthy',
            'service': 'command-service',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'service': 'command-service',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# Endpoint 1: Create Product
@app.route('/api/products', methods=['POST'])
def create_product():
    """
    Create a new product
    Request body: { "name": "string", "category": "string", "price": number, "stock": integer }
    """
    try:
        # Parse request body
        data = request.get_json()
        
        # Validate required fields
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        name = data.get('name')
        category = data.get('category')
        price = data.get('price')
        stock = data.get('stock')
        
        # Validate each field
        if not name:
            return jsonify({'error': 'name is required'}), 400
        if not category:
            return jsonify({'error': 'category is required'}), 400
        if price is None:
            return jsonify({'error': 'price is required'}), 400
        if stock is None:
            return jsonify({'error': 'stock is required'}), 400
        
        # Validate data types and values
        try:
            price = float(price)
            stock = int(stock)
            if price < 0:
                return jsonify({'error': 'price must be >= 0'}), 400
            if stock < 0:
                return jsonify({'error': 'stock must be >= 0'}), 400
        except ValueError:
            return jsonify({'error': 'price must be a number, stock must be an integer'}), 400
        
        # Insert into database and create event in outbox
        with engine.begin() as conn:  # BEGIN TRANSACTION
            # Insert product
            result = conn.execute(
                text("""
                    INSERT INTO products (name, category, price, stock)
                    VALUES (:name, :category, :price, :stock)
                    RETURNING id
                """),
                {"name": name, "category": category, "price": price, "stock": stock}
            )
            product_id = result.fetchone()[0]
            
            # Create event payload
            event_payload = {
                "eventType": "ProductCreated",
                "productId": product_id,
                "name": name,
                "category": category,
                "price": price,
                "stock": stock,
                "timestamp": datetime.now().isoformat()
            }
            
            # Insert into outbox table (same transaction)
            conn.execute(
                text("""
                    INSERT INTO outbox (id, topic, payload, created_at, published_at)
                    VALUES (:id, :topic, :payload, :created_at, NULL)
                """),
                {
                    "id": uuid.uuid4(),
                    "topic": "product-events",
                    "payload": json.dumps(event_payload),
                    "created_at": datetime.now()
                }
            )
            
            logger.info(f"Product created: {product_id} - {name}")
        
        # Return success response
        return jsonify({
            'productId': product_id,
            'message': 'Product created successfully'
        }), 201
        
    except IntegrityError as e:
        logger.error(f"Database integrity error: {e}")
        return jsonify({'error': 'Database constraint violation'}), 400
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint 2: Create Order
@app.route('/api/orders', methods=['POST'])
def create_order():
    """
    Create a new order
    Request body: { "customerId": integer, "items": [{"productId": integer, "quantity": integer, "price": number}] }
    """
    try:
        # Parse request body
        data = request.get_json()
        
        # Validate required fields
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        customer_id = data.get('customerId')
        items = data.get('items', [])
        
        if not customer_id:
            return jsonify({'error': 'customerId is required'}), 400
        if not items or len(items) == 0:
            return jsonify({'error': 'items cannot be empty'}), 400
        
        # Validate customer exists
        with engine.connect() as conn:
            customer = conn.execute(
                text("SELECT id FROM customers WHERE id = :customer_id"),
                {"customer_id": customer_id}
            ).fetchone()
            
            if not customer:
                return jsonify({'error': f'Customer {customer_id} not found'}), 404
        
        # Begin transaction for order creation
        with engine.begin() as conn:
            # Calculate total and validate stock
            total = 0
            order_items_data = []
            product_stock_updates = []
            
            for item in items:
                product_id = item.get('productId')
                quantity = item.get('quantity')
                price = item.get('price')
                
                # Validate item fields
                if not product_id:
                    return jsonify({'error': 'productId is required for each item'}), 400
                if not quantity or quantity <= 0:
                    return jsonify({'error': 'quantity must be positive'}), 400
                if not price or price <= 0:
                    return jsonify({'error': 'price must be positive'}), 400
                
                # Get current product stock
                product = conn.execute(
                    text("SELECT id, name, price, stock FROM products WHERE id = :product_id FOR UPDATE"),
                    {"product_id": product_id}
                ).fetchone()
                
                if not product:
                    return jsonify({'error': f'Product {product_id} not found'}), 404
                
                # Check stock availability
                if product[3] < quantity:  # product[3] is stock
                    return jsonify({
                        'error': f'Insufficient stock for product {product[1]}. Available: {product[3]}, Requested: {quantity}'
                    }), 400
                
                # Calculate item total
                item_total = quantity * price
                total += item_total
                
                # Store for later use
                order_items_data.append({
                    'product_id': product_id,
                    'quantity': quantity,
                    'price': price
                })
                
                # Prepare stock update
                product_stock_updates.append({
                    'product_id': product_id,
                    'new_stock': product[3] - quantity
                })
            
            # Insert order
            result = conn.execute(
                text("""
                    INSERT INTO orders (customer_id, total, status, created_at, updated_at)
                    VALUES (:customer_id, :total, 'pending', :created_at, :updated_at)
                    RETURNING id
                """),
                {
                    "customer_id": customer_id,
                    "total": total,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                }
            )
            order_id = result.fetchone()[0]
            
            # Insert order items and update stock
            for item_data in order_items_data:
                conn.execute(
                    text("""
                        INSERT INTO order_items (order_id, product_id, quantity, price)
                        VALUES (:order_id, :product_id, :quantity, :price)
                    """),
                    {
                        "order_id": order_id,
                        "product_id": item_data['product_id'],
                        "quantity": item_data['quantity'],
                        "price": item_data['price']
                    }
                )
            
            # Update product stock
            for update in product_stock_updates:
                conn.execute(
                    text("UPDATE products SET stock = :new_stock, updated_at = :updated_at WHERE id = :product_id"),
                    {
                        "new_stock": update['new_stock'],
                        "updated_at": datetime.now(),
                        "product_id": update['product_id']
                    }
                )
            
            # Create OrderCreated event payload
            event_payload = {
                "eventType": "OrderCreated",
                "orderId": order_id,
                "customerId": customer_id,
                "total": total,
                "items": [
                    {
                        "productId": item['product_id'],
                        "quantity": item['quantity'],
                        "price": item['price']
                    }
                    for item in order_items_data
                ],
                "timestamp": datetime.now().isoformat()
            }
            
            # Insert into outbox table
            conn.execute(
                text("""
                    INSERT INTO outbox (id, topic, payload, created_at, published_at)
                    VALUES (:id, :topic, :payload, :created_at, NULL)
                """),
                {
                    "id": uuid.uuid4(),
                    "topic": "order-events",
                    "payload": json.dumps(event_payload),
                    "created_at": datetime.now()
                }
            )
            
            logger.info(f"Order created: {order_id} for customer {customer_id}, total: {total}")
        
        # Return success response
        return jsonify({
            'orderId': order_id,
            'message': 'Order created successfully'
        }), 201
        
    except IntegrityError as e:
        logger.error(f"Database integrity error: {e}")
        return jsonify({'error': 'Database constraint violation'}), 400
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Get all products (helper endpoint for testing)
@app.route('/api/products', methods=['GET'])
def get_products():
    """Get all products (for testing)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, name, category, price, stock FROM products"))
            products = []
            for row in result:
                products.append({
                    'id': row[0],
                    'name': row[1],
                    'category': row[2],
                    'price': float(row[3]),
                    'stock': row[4]
                })
            return jsonify(products), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Get all customers (helper endpoint for testing)
@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Get all customers (for testing)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, name, email FROM customers"))
            customers = []
            for row in result:
                customers.append({
                    'id': row[0],
                    'name': row[1],
                    'email': row[2]
                })
            return jsonify(customers), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"Starting Command Service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)