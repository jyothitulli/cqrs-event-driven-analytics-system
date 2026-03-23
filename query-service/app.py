"""
Query Service - Serves analytics data from materialized views
Provides REST API endpoints for e-commerce analytics dashboard
"""
import os
import logging
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Database configuration
READ_DATABASE_URL = os.environ.get('READ_DATABASE_URL', 'postgresql://user:password@read-db:5432/read_db')
logger.info(f"Connecting to read database: {READ_DATABASE_URL}")

# Create database engine
engine = create_engine(READ_DATABASE_URL, pool_pre_ping=True)

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    """Check if service is healthy"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return jsonify({
            'status': 'healthy',
            'service': 'query-service',
            'database': 'connected',
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

# Endpoint 1: Product Sales Analytics
@app.route('/api/analytics/products/<int:product_id>/sales', methods=['GET'])
def get_product_sales(product_id):
    """
    Get sales analytics for a specific product
    Returns: total_quantity_sold, total_revenue, order_count
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT product_id, product_name, total_quantity_sold, total_revenue, order_count
                    FROM product_sales_view
                    WHERE product_id = :product_id
                """),
                {"product_id": product_id}
            ).fetchone()
            
            if not result:
                return jsonify({
                    'error': f'Product {product_id} not found in analytics'
                }), 404
            
            return jsonify({
                'productId': result[0],
                'productName': result[1],
                'totalQuantitySold': result[2],
                'totalRevenue': float(result[3]),
                'orderCount': result[4]
            }), 200
            
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint 2: Category Revenue Analytics
@app.route('/api/analytics/categories/<category>/revenue', methods=['GET'])
def get_category_revenue(category):
    """
    Get revenue analytics for a specific category
    Returns: total_revenue, total_orders
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT category_name, total_revenue, total_orders
                    FROM category_metrics_view
                    WHERE category_name = :category
                """),
                {"category": category}
            ).fetchone()
            
            if not result:
                # Return zero values for categories with no data
                return jsonify({
                    'category': category,
                    'totalRevenue': 0,
                    'totalOrders': 0,
                    'message': 'No data available for this category'
                }), 200
            
            return jsonify({
                'category': result[0],
                'totalRevenue': float(result[1]),
                'totalOrders': result[2]
            }), 200
            
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint 3: Customer Lifetime Value
@app.route('/api/analytics/customers/<int:customer_id>/lifetime-value', methods=['GET'])
def get_customer_ltv(customer_id):
    """
    Get lifetime value metrics for a specific customer
    Returns: total_spent, order_count, average_order_value, first_order_date, last_order_date
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT customer_id, customer_name, customer_email, total_spent, 
                           order_count, average_order_value, first_order_date, last_order_date
                    FROM customer_ltv_view
                    WHERE customer_id = :customer_id
                """),
                {"customer_id": customer_id}
            ).fetchone()
            
            if not result:
                return jsonify({
                    'error': f'Customer {customer_id} not found in analytics',
                    'message': 'No orders have been placed by this customer yet'
                }), 404
            
            return jsonify({
                'customerId': result[0],
                'customerName': result[1],
                'customerEmail': result[2],
                'totalSpent': float(result[3]),
                'orderCount': result[4],
                'averageOrderValue': float(result[5]),
                'firstOrderDate': result[6].isoformat() if result[6] else None,
                'lastOrderDate': result[7].isoformat() if result[7] else None
            }), 200
            
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Endpoint 4: Sync Status (Eventual Consistency Lag)
@app.route('/api/analytics/sync-status', methods=['GET'])
def get_sync_status():
    """
    Report the lag between write and read models
    Returns: last_processed_event_timestamp, lag_seconds
    """
    try:
        # Get the most recently processed event
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT event_id, event_type, processed_at
                    FROM processed_events
                    ORDER BY processed_at DESC
                    LIMIT 1
                """)
            ).fetchone()
            
            if not result:
                return jsonify({
                    'lastProcessedEventTimestamp': None,
                    'lagSeconds': None,
                    'message': 'No events have been processed yet'
                }), 200
            
            last_processed = result[2]
            now = datetime.now()
            
            # Calculate lag in seconds
            if last_processed:
                lag = (now - last_processed).total_seconds()
            else:
                lag = None
            
            return jsonify({
                'lastProcessedEventId': result[0],
                'lastProcessedEventType': result[1],
                'lastProcessedEventTimestamp': last_processed.isoformat() if last_processed else None,
                'lagSeconds': round(lag, 2) if lag else None,
                'currentTime': now.isoformat()
            }), 200
            
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        return jsonify({'error': 'Database error occurred'}), 500
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return jsonify({'error': 'Internal server error'}), 500

# Additional Endpoint: Get all products (for testing)
@app.route('/api/products', methods=['GET'])
def get_products():
    """Get all products from read database (for testing)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT product_id, product_name, total_quantity_sold, total_revenue FROM product_sales_view")
            )
            products = []
            for row in result:
                products.append({
                    'productId': row[0],
                    'productName': row[1],
                    'totalQuantitySold': row[2],
                    'totalRevenue': float(row[3])
                })
            return jsonify(products), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Additional Endpoint: Get all customers (for testing)
@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Get all customers from read database (for testing)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT customer_id, customer_name, customer_email, total_spent, order_count FROM customer_ltv_view")
            )
            customers = []
            for row in result:
                customers.append({
                    'customerId': row[0],
                    'customerName': row[1],
                    'customerEmail': row[2],
                    'totalSpent': float(row[3]),
                    'orderCount': row[4]
                })
            return jsonify(customers), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8081))
    logger.info(f"Starting Query Service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)