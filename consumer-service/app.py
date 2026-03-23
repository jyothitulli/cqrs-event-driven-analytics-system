"""
Consumer Service - Processes events from RabbitMQ and updates materialized views
Implements idempotency to prevent duplicate processing
"""
import os
import json
import threading
import time
import logging
from datetime import datetime
from flask import Flask, jsonify
import pika
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app for health checks
app = Flask(__name__)

# Database configuration
READ_DATABASE_URL = os.environ.get('READ_DATABASE_URL', 'postgresql://user:password@read-db:5432/read_db')
WRITE_DATABASE_URL = os.environ.get('WRITE_DATABASE_URL', 'postgresql://user:password@write-db:5432/write_db')
BROKER_URL = os.environ.get('BROKER_URL', 'amqp://guest:guest@broker:5672/')

logger.info(f"Connecting to read database: {READ_DATABASE_URL}")
logger.info(f"Connecting to write database: {WRITE_DATABASE_URL}")
logger.info(f"Connecting to broker: {BROKER_URL}")

# Create database engines
read_engine = create_engine(READ_DATABASE_URL, pool_pre_ping=True)
write_engine = create_engine(WRITE_DATABASE_URL, pool_pre_ping=True)

# Global status tracking
consumer_running = True
last_processed_event_time = None
last_processed_event_id = None

def create_rabbitmq_connection():
    """Create a RabbitMQ connection with proper heartbeat settings"""
    try:
        params = pika.URLParameters(BROKER_URL)
        params.heartbeat = 30  # Send heartbeat every 30 seconds
        params.blocked_connection_timeout = 60
        params.connection_attempts = 3
        params.retry_delay = 5
        params.socket_timeout = 10
        
        connection = pika.BlockingConnection(params)
        logger.info("RabbitMQ connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

# Health check endpoint
@app.route('/health', methods=['GET'])
def health_check():
    """Check if service is healthy"""
    try:
        # Test database connection
        with read_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return jsonify({
            'status': 'healthy',
            'service': 'consumer-service',
            'consumer_running': consumer_running,
            'last_processed_event_time': last_processed_event_time,
            'last_processed_event_id': last_processed_event_id,
            'timestamp': datetime.now().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

def mark_event_processed(event_id, event_type):
    """Mark an event as processed in the processed_events table (idempotency)"""
    try:
        with read_engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO processed_events (event_id, event_type, processed_at)
                    VALUES (:event_id, :event_type, :processed_at)
                    ON CONFLICT (event_id) DO NOTHING
                """),
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "processed_at": datetime.now()
                }
            )
            return True
    except Exception as e:
        logger.error(f"Error marking event {event_id} as processed: {e}")
        return False

def is_event_processed(event_id):
    """Check if an event has already been processed"""
    try:
        with read_engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM processed_events WHERE event_id = :event_id"),
                {"event_id": event_id}
            ).fetchone()
            return result is not None
    except Exception as e:
        logger.error(f"Error checking if event {event_id} is processed: {e}")
        return False

def update_product_sales(product_id, product_name, quantity, revenue):
    """Update product_sales_view with new order data"""
    try:
        with read_engine.begin() as conn:
            result = conn.execute(
                text("SELECT product_id FROM product_sales_view WHERE product_id = :product_id"),
                {"product_id": product_id}
            ).fetchone()
            
            if result:
                conn.execute(
                    text("""
                        UPDATE product_sales_view 
                        SET total_quantity_sold = total_quantity_sold + :quantity,
                            total_revenue = total_revenue + :revenue,
                            order_count = order_count + 1,
                            product_name = :product_name
                        WHERE product_id = :product_id
                    """),
                    {
                        "product_id": product_id,
                        "quantity": quantity,
                        "revenue": revenue,
                        "product_name": product_name
                    }
                )
                logger.info(f"Updated product_sales_view for product {product_id}")
            else:
                conn.execute(
                    text("""
                        INSERT INTO product_sales_view (product_id, product_name, total_quantity_sold, total_revenue, order_count)
                        VALUES (:product_id, :product_name, :quantity, :revenue, 1)
                    """),
                    {
                        "product_id": product_id,
                        "product_name": product_name,
                        "quantity": quantity,
                        "revenue": revenue
                    }
                )
                logger.info(f"Inserted new product into product_sales_view: {product_id}")
    except Exception as e:
        logger.error(f"Error updating product_sales_view: {e}")
        raise

def update_customer_ltv(customer_id, customer_name, customer_email, total, order_date):
    """Update customer_ltv_view with new order data"""
    try:
        with read_engine.begin() as conn:
            result = conn.execute(
                text("SELECT customer_id FROM customer_ltv_view WHERE customer_id = :customer_id"),
                {"customer_id": customer_id}
            ).fetchone()
            
            if result:
                conn.execute(
                    text("""
                        UPDATE customer_ltv_view 
                        SET total_spent = total_spent + :total,
                            order_count = order_count + 1,
                            average_order_value = (total_spent + :total) / (order_count + 1),
                            last_order_date = :order_date
                        WHERE customer_id = :customer_id
                    """),
                    {
                        "customer_id": customer_id,
                        "total": total,
                        "order_date": order_date
                    }
                )
                logger.info(f"Updated customer_ltv_view for customer {customer_id}")
            else:
                conn.execute(
                    text("""
                        INSERT INTO customer_ltv_view (customer_id, customer_name, customer_email, total_spent, order_count, average_order_value, first_order_date, last_order_date)
                        VALUES (:customer_id, :customer_name, :customer_email, :total, 1, :total, :order_date, :order_date)
                    """),
                    {
                        "customer_id": customer_id,
                        "customer_name": customer_name,
                        "customer_email": customer_email,
                        "total": total,
                        "order_date": order_date
                    }
                )
                logger.info(f"Inserted new customer into customer_ltv_view: {customer_id}")
    except Exception as e:
        logger.error(f"Error updating customer_ltv_view: {e}")
        raise

def update_hourly_sales(hour_timestamp, revenue, quantity):
    """Update hourly_sales_view with new order data"""
    try:
        with read_engine.begin() as conn:
            result = conn.execute(
                text("SELECT hour_timestamp FROM hourly_sales_view WHERE hour_timestamp = :hour_timestamp"),
                {"hour_timestamp": hour_timestamp}
            ).fetchone()
            
            if result:
                conn.execute(
                    text("""
                        UPDATE hourly_sales_view 
                        SET total_revenue = total_revenue + :revenue,
                            total_orders = total_orders + 1,
                            total_items_sold = total_items_sold + :quantity
                        WHERE hour_timestamp = :hour_timestamp
                    """),
                    {"hour_timestamp": hour_timestamp, "revenue": revenue, "quantity": quantity}
                )
                logger.info(f"Updated hourly_sales_view for {hour_timestamp}")
            else:
                conn.execute(
                    text("""
                        INSERT INTO hourly_sales_view (hour_timestamp, total_revenue, total_orders, total_items_sold)
                        VALUES (:hour_timestamp, :revenue, 1, :quantity)
                    """),
                    {"hour_timestamp": hour_timestamp, "revenue": revenue, "quantity": quantity}
                )
                logger.info(f"Inserted new hour into hourly_sales_view: {hour_timestamp}")
    except Exception as e:
        logger.error(f"Error updating hourly_sales_view: {e}")
        raise

def handle_order_created(event_id, payload):
    """Handle OrderCreated event"""
    logger.info(f"Processing OrderCreated event {event_id}")
    
    order_id = payload.get('orderId')
    customer_id = payload.get('customerId')
    total = payload.get('total')
    items = payload.get('items', [])
    timestamp = payload.get('timestamp')
    
    # Parse timestamp for hourly aggregation
    if timestamp:
        try:
            order_datetime = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            hour_timestamp = order_datetime.replace(minute=0, second=0, microsecond=0)
        except:
            hour_timestamp = datetime.now().replace(minute=0, second=0, microsecond=0)
    else:
        hour_timestamp = datetime.now().replace(minute=0, second=0, microsecond=0)
    
    # Update materialized views for each item
    for item in items:
        product_id = item.get('productId')
        quantity = item.get('quantity')
        price = item.get('price')
        revenue = quantity * price
        
        # Get product name from write DB
        product_name = f"Product_{product_id}"
        
        # Update product sales
        update_product_sales(product_id, product_name, quantity, revenue)
        
        # Update hourly sales
        update_hourly_sales(hour_timestamp, revenue, quantity)
    
    # Update customer LTV
    customer_name = f"Customer_{customer_id}"
    customer_email = f"customer{customer_id}@example.com"
    update_customer_ltv(customer_id, customer_name, customer_email, total, timestamp)
    
    logger.info(f"Completed processing OrderCreated event {event_id}")

def handle_product_created(event_id, payload):
    """Handle ProductCreated event"""
    logger.info(f"Processing ProductCreated event {event_id}")
    
    product_id = payload.get('productId')
    name = payload.get('name')
    
    # Initialize product in product_sales_view with zero sales
    try:
        with read_engine.begin() as conn:
            result = conn.execute(
                text("SELECT product_id FROM product_sales_view WHERE product_id = :product_id"),
                {"product_id": product_id}
            ).fetchone()
            
            if not result:
                conn.execute(
                    text("""
                        INSERT INTO product_sales_view (product_id, product_name, total_quantity_sold, total_revenue, order_count)
                        VALUES (:product_id, :product_name, 0, 0, 0)
                    """),
                    {"product_id": product_id, "product_name": name}
                )
                logger.info(f"Initialized product_sales_view for new product {product_id}")
    except Exception as e:
        logger.error(f"Error initializing product in view: {e}")
        raise
    
    logger.info(f"Completed processing ProductCreated event {event_id}")

def process_event(event_id, topic, payload_data):
    """Process a single event based on its type"""
    try:
        # Check idempotency
        if is_event_processed(event_id):
            logger.info(f"Event {event_id} already processed, skipping")
            return True
        
        # Handle payload - it might be string or dict
        if isinstance(payload_data, dict):
            payload = payload_data
        elif isinstance(payload_data, str):
            payload = json.loads(payload_data)
        else:
            payload = json.loads(str(payload_data))
        
        event_type = payload.get('eventType')
        
        logger.info(f"Processing event {event_id} of type {event_type}")
        
        # Handle based on event type
        if event_type == 'OrderCreated':
            handle_order_created(event_id, payload)
        elif event_type == 'ProductCreated':
            handle_product_created(event_id, payload)
        else:
            logger.warning(f"Unknown event type: {event_type}")
            return False
        
        # Mark as processed
        mark_event_processed(event_id, event_type)
        
        # Update global status
        global last_processed_event_time, last_processed_event_id
        last_processed_event_time = datetime.now().isoformat()
        last_processed_event_id = event_id
        
        logger.info(f"Successfully processed event {event_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing event {event_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def outbox_publisher():
    """Background thread that polls outbox and publishes events to RabbitMQ"""
    global consumer_running
    
    logger.info("Starting outbox publisher...")
    
    while consumer_running:
        connection = None
        channel = None
        
        try:
            # Connect to RabbitMQ
            connection = create_rabbitmq_connection()
            channel = connection.channel()
            
            # Declare queues
            channel.queue_declare(queue='order-events', durable=True)
            channel.queue_declare(queue='product-events', durable=True)
            
            logger.info("Publisher connected to RabbitMQ successfully")
            
            while consumer_running:
                try:
                    # Fetch unpublished events from outbox
                    with write_engine.connect() as conn:
                        result = conn.execute(
                            text("""
                                SELECT id, topic, payload 
                                FROM outbox 
                                WHERE published_at IS NULL 
                                ORDER BY created_at ASC 
                                LIMIT 10
                            """)
                        )
                        events = result.fetchall()
                    
                    if not events:
                        time.sleep(2)
                        continue
                    
                    # Publish each event
                    for event in events:
                        event_id = str(event[0])
                        topic = event[1]
                        payload = event[2]
                        
                        # IMPORTANT FIX: Convert payload to string if it's a dict
                        if isinstance(payload, dict):
                            payload_str = json.dumps(payload)
                        else:
                            payload_str = payload
                        
                        message = json.dumps({
                            'event_id': event_id,
                            'payload': payload_str
                        })
                        
                        if topic == 'order-events':
                            channel.basic_publish(
                                exchange='',
                                routing_key='order-events',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                        elif topic == 'product-events':
                            channel.basic_publish(
                                exchange='',
                                routing_key='product-events',
                                body=message,
                                properties=pika.BasicProperties(delivery_mode=2)
                            )
                        else:
                            logger.warning(f"Unknown topic: {topic}")
                            continue
                        
                        # Mark as published
                        with write_engine.begin() as conn:
                            conn.execute(
                                text("UPDATE outbox SET published_at = :published_at WHERE id = :id"),
                                {"published_at": datetime.now(), "id": event_id}
                            )
                        
                        logger.info(f"Published event {event_id} to {topic}")
                    
                except Exception as e:
                    logger.error(f"Error in publisher loop: {e}")
                    time.sleep(5)
                    if "connection" in str(e).lower():
                        break
                    
        except Exception as e:
            logger.error(f"Publisher connection error: {e}")
            time.sleep(10)
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except:
                    pass

def rabbitmq_consumer():
    """Background thread that consumes events from RabbitMQ"""
    global consumer_running
    
    logger.info("Starting RabbitMQ consumer...")
    
    while consumer_running:
        connection = None
        channel = None
        
        try:
            # Connect to RabbitMQ
            connection = create_rabbitmq_connection()
            channel = connection.channel()
            
            # Declare queues
            channel.queue_declare(queue='order-events', durable=True)
            channel.queue_declare(queue='product-events', durable=True)
            
            logger.info("Consumer connected to RabbitMQ successfully")
            
            def order_callback(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    event_id = message.get('event_id')
                    payload_data = message.get('payload')
                    
                    if event_id and payload_data:
                        process_event(event_id, 'order-events', payload_data)
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error(f"Error in order callback: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            def product_callback(ch, method, properties, body):
                try:
                    message = json.loads(body)
                    event_id = message.get('event_id')
                    payload_data = message.get('payload')
                    
                    if event_id and payload_data:
                        process_event(event_id, 'product-events', payload_data)
                    
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error(f"Error in product callback: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            channel.basic_consume(queue='order-events', on_message_callback=order_callback, auto_ack=False)
            channel.basic_consume(queue='product-events', on_message_callback=product_callback, auto_ack=False)
            
            logger.info("Starting to consume messages...")
            channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            time.sleep(10)
        finally:
            if connection and connection.is_open:
                try:
                    connection.close()
                except:
                    pass

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    
    # Start outbox publisher in background thread
    publisher_thread = threading.Thread(target=outbox_publisher, daemon=True)
    publisher_thread.start()
    logger.info("Started outbox publisher thread")
    
    # Start RabbitMQ consumer in background thread
    consumer_thread = threading.Thread(target=rabbitmq_consumer, daemon=True)
    consumer_thread.start()
    logger.info("Started RabbitMQ consumer thread")
    
    # Run Flask app for health checks
    logger.info(f"Starting consumer service on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)