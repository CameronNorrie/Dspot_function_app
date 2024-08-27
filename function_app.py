import azure.functions as func
import requests
import os
from datetime import datetime, timedelta
from pony.orm import *
import logging
import json

# Initialize the Database
db = Database()
logging.info('Initializing DB Connector Object')

class SquareAuth(db.Entity):
   access_token = Optional(str)
   refresh_token = PrimaryKey(str)
   client_id = Required(str)
   client_secret = Required(str)
   token_expiry = Optional(datetime)

class FoodTruckData(db.Entity):
   revenue_center_desc = Required(str)
   order_id = Required(str)
   item_order_time = Required(str)
   item_number = Required(str)
   item_name = Required(str)
   item_quantity = Required(float)
   item_gross_sales = Required(float)
   item_net_sales = Required(float)
   tip_amount = Required(float)
   store_id = Required(str)
   uid = Required(str)

# Azure Function App
app = func.FunctionApp()

# Connect to the Database and create new tables
logging.info('Connecting to Database')
try:
   db.bind(provider='postgres', user=os.environ.get('database_user'), password=os.environ.get('database_pass'), host=os.environ.get('database_host'), database='postgres')
   db.generate_mapping(create_tables=True)
except Exception as e:
   logging.error("Could not connect to database: " + str(e))
   raise e
logging.info('Connected to Database')

@app.function_name(route="get_orders")
@app.schedule(schedule="0 30 7 * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
def get_orders(mytimer: func.TimerRequest):
   logging.info('get_orders function processed a request.')

   # Square access token
   access_token = os.environ.get('access_token')

   headers = {
      "Authorization": f"Bearer {access_token}",
      "Content-Type": "application/json",
      "Square-Version": "2024-07-17"
   }

   # Square Locations API endpoint
   locations_url = "https://connect.squareup.com/v2/locations"
   location_response = requests.get(locations_url, headers=headers)
   if location_response.status_code != 200:
      logging.error(f"Failed to fetch locations: {location_response.text}")
      return func.HttpResponse(f"Failed to fetch locations: {location_response.text}", status_code=location_response.status_code)

   # Organize the locations into a list
   locations = location_response.json().get('locations', [])
   location_ids = [location['id'] for location in locations]

   # Get the last fetch time from the database
   with db_session:
      last_fetch_time = select(max(o.item_order_time) for o in FoodTruckData).first()

   start_time = last_fetch_time.isoformat() + "Z"

   # Set the end time to now
   end_time = datetime.now().replace(microsecond=0).isoformat() + "Z"
   
   for location_id in location_ids:
      cursor = None
      while True:
         #parameters for the order search
         params = {
            "location_ids": [location_id],
            "query": {
               "filter": {
                  "date_time_filter": {
                     "created_at": {
                        "start_at": start_time,
                        "end_at": end_time
                     }
                  }
               }
            },
            "cursor": cursor  # Use the cursor for pagination
         }

         # Fetch orders from Square API
         order_url = 'https://connect.squareup.com/v2/orders/search'
         order_response = requests.post(order_url, headers=headers, json=params)
         if order_response.status_code != 200:
            logging.error(f"Failed to fetch orders: {order_response.text}")
            return func.HttpResponse(f"Failed to fetch orders: {order_response.text}", status_code=order_response.status_code)

         # Organize the orders into a list
         data = order_response.json()
         orders = data.get('orders', [])
         
         for order in orders:

            #grab the orders tip amount and the order time
            created_at = order.get("created_at")
            tip_amount = order.get("total_tip_money", {}).get("amount", 0) / 100
            location_id = order.get("location_id")
            order_id = order.get("id")

            for line_item in order.get("line_items", []):
               # Convert gross and net sales amounts to dollars
               item_gross_sales = line_item.get("gross_sales_money", {}).get("amount", 0) / 100
               item_net_sales = line_item.get("total_money", {}).get("amount", 0) / 100
               #for duplicate check
               uid = line_item.get("uid")

               # Check if the record already exists
               with db_session:
                  existing_record = FoodTruckData.get(uid=uid, item_order_time=created_at)
                  if not existing_record:
                     # Insert new record
                     FoodTruckData(
                        revenue_center_desc="Food Truck",
                        order_id = order_id,
                        item_order_time = created_at, 
                        item_number = str(line_item.get("catalog_object_id")),
                        item_name = str(line_item.get("name")),
                        item_quantity = line_item.get("quantity"),
                        item_gross_sales = item_gross_sales,
                        item_net_sales = item_net_sales,
                        tip_amount = tip_amount,
                        store_id = location_id,
                        uid = uid
                     )
                  commit()
                  
         # Check if there's more data to fetch
         cursor = data.get('cursor')
         if not cursor:
            break

   return func.HttpResponse(
      "All orders have been fetched successfully",
      status_code=200
   )
