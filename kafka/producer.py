"""
Kafka Producer for Real-Time Stock Data
This script fetches stock prices from Alpha Vantage API every 60 seconds
and streams them to Kafka topic 'stock_prices'
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import requests
from datetime import datetime
import time
import os
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

# Configure logging for debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'stock_prices'

# Stock symbols to track (top 5 US tech stocks)
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

def create_producer():
    """
    Create and configure Kafka producer
    Returns: KafkaProducer object
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            # Serialize data to JSON bytes
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Ensure message is written to Kafka before confirming
            acks='all',
            # Retry up to 3 times on failure
            retries=3,
            # Batch messages for efficiency
            linger_ms=10
        )
        logger.info("✅ Kafka Producer created successfully")
        return producer
    except KafkaError as e:
        logger.error(f"❌ Failed to create Kafka producer: {e}")
        raise

def fetch_stock_data(symbol):
    """
    Fetch real-time stock data from Alpha Vantage API
    
    Args:
        symbol (str): Stock ticker symbol (e.g., 'AAPL')
    
    Returns:
        dict: Stock data or None if failed
    """
    # Alpha Vantage API endpoint
    url = f"https://www.alphavantage.co/query"
    params = {
        'function': 'GLOBAL_QUOTE',  # Real-time quote
        'symbol': symbol,
        'apikey': API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # Raise error for bad status codes
        data = response.json()
        
        # Check if API returned valid data
        if 'Global Quote' in data and data['Global Quote']:
            quote = data['Global Quote']
            return {
                'symbol': symbol,
                'price': float(quote.get('05. price', 0)),
                'volume': int(quote.get('06. volume', 0)),
                'change_percent': quote.get('10. change percent', '0%').replace('%', ''),
                'timestamp': datetime.utcnow().isoformat(),
                'trade_date': quote.get('07. latest trading day', '')
            }
        else:
            logger.warning(f"⚠️  No data returned for {symbol}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed for {symbol}: {e}")
        return None
    except (KeyError, ValueError) as e:
        logger.error(f"❌ Data parsing failed for {symbol}: {e}")
        return None

def send_to_kafka(producer, topic, message):
    """
    Send message to Kafka topic
    
    Args:
        producer: KafkaProducer object
        topic (str): Kafka topic name
        message (dict): Data to send
    """
    try:
        # Send message asynchronously
        future = producer.send(topic, value=message)
        # Wait for confirmation
        record_metadata = future.get(timeout=10)
        
        logger.info(
            f"📤 Sent: {message['symbol']} @ ${message['price']:.2f} "
            f"→ Topic: {record_metadata.topic}, "
            f"Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}"
        )
        return True
        
    except KafkaError as e:
        logger.error(f"❌ Failed to send message: {e}")
        return False

def stream_stock_data():
    """
    Main function: Continuously fetch and stream stock data
    """
    logger.info("🚀 Starting Stock Data Producer...")
    
    # Create Kafka producer
    producer = create_producer()
    
    # Counter for tracking
    message_count = 0
    
    try:
        while True:
            logger.info(f"\n📊 Fetching data for {len(STOCK_SYMBOLS)} stocks...")
            
            for symbol in STOCK_SYMBOLS:
                # Fetch stock data
                stock_data = fetch_stock_data(symbol)
                
                if stock_data:
                    # Send to Kafka
                    if send_to_kafka(producer, KAFKA_TOPIC, stock_data):
                        message_count += 1
                
                # Small delay to avoid API rate limits (5 calls/min free tier)
                time.sleep(12)  # 12s * 5 stocks = 60 seconds total
            
            logger.info(f"✅ Batch complete. Total messages sent: {message_count}")
            logger.info("⏳ Waiting 60 seconds before next batch...\n")
            
            # Wait before next cycle (already waited 60s in the loop)
            # time.sleep(0)  # No additional wait needed
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping producer (Ctrl+C pressed)")
    finally:
        # Clean shutdown
        producer.flush()  # Ensure all messages are sent
        producer.close()
        logger.info("👋 Producer closed gracefully")

if __name__ == "__main__":
    # Check if API key is configured
    if not API_KEY:
        logger.error("❌ ALPHA_VANTAGE_API_KEY not found in .env file!")
        exit(1)
    
    # Start streaming
    stream_stock_data()

