import yfinance as yf
from pymongo import MongoClient
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

# Function to fetch and store data
def fetch_and_store_data():
    # Connect to MongoDB
    client = MongoClient("mongodb+srv://amiarnab:amiarnab100@cluster0.fugun.mongodb.net/?retryWrites=true&w=majority")
    db = client['stock_data']
    collection = db['ICICIBANK']

    # Get the current time
    now = datetime.now()

    # Start time at 11.15 AM
    start_time = now.replace(hour=11, minute=15, second=0, microsecond=0)

    # End time at 2.15 PM
    end_time = now.replace(hour=14, minute=15, second=0, microsecond=0)

    if start_time <= now <= end_time:
        # Create a Ticker instance for ICICI Bank
        icici = yf.Ticker("ICICIBANK.NS")

        # Get the historical data for the last 15 minutes
        data = icici.history(period="15m")

        if not data.empty:
            # Store the data in MongoDB
            collection.insert_many(data.to_dict(orient='records'))

if __name__=="__main__":
    # Create a scheduler
    scheduler = BlockingScheduler()

    # Schedule the job to run every 15 minutes
    scheduler.add_job(fetch_and_store_data, 'interval', minutes=15)

    # Start the scheduler
    scheduler.start()
