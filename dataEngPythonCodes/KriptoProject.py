import mysql.connector
from mysql.connector import errorcode
import requests
import json
import time, threading

# Portfolio table data
# asset_id |asset_count
# ---------------------
# ADA      |10000.0
# ATOM     |4000.0
# ETH      |70.0
# USDT     |50000.0
# XRP      |140000.0
def insert_into_exchanges(exchange_id, name, website, volume_24h):
    try:
               
        mySql_insert_query = """INSERT INTO exchanges (exchange_id, name, website, volume_24h) 
                                VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE volume_24h= VALUES(volume_24h)"""

        record = (exchange_id, name, website, volume_24h)
        
        cursor.execute(mySql_insert_query, record)
        connection.commit()        

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))
 

        
def insert_into_markets(exchange_id, symbol, base_asset, quote_asset, 
                        price_unconverted, price, change_24h, spread, volume_24h, status, created_at, updated_at ):
    try:
        
        mySql_insert_query = """INSERT INTO markets (exchange_id, symbol, base_asset, quote_asset, price_unconverted,
        price, change_24h, spread, volume_24h, status, created_at, updated_at) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """

        record = (exchange_id, symbol, base_asset, quote_asset, price_unconverted,
        price, change_24h, spread, volume_24h, status, created_at, updated_at)
        
        cursor.execute(mySql_insert_query, record)
        connection.commit()        

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))
    
     
    
def insert_into_assets(asset_id, name, price, volume_24h, 
                        change_1h, change_24h, change_7d, status,created_at, updated_at ):
    try:
       
        mySql_insert_query = """INSERT INTO assets (asset_id, name, price, volume_24h, 
                        change_1h, change_24h, change_7d, status,created_at, updated_at) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE 
                                price= VALUES(price), volume_24h= VALUES(volume_24h), change_1h=VALUES(change_1h),
                                change_24h=VALUES(change_24h), change_7d=VALUES(change_7d), status=VALUES(status),
                                created_at=VALUES(created_at), updated_at=VALUES(updated_at)"""

        record = (asset_id, name, price, volume_24h, 
                        change_1h, change_24h, change_7d, status,created_at, updated_at)
        
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        

    except mysql.connector.Error as error:
        print("Failed to insert into MySQL table {}".format(error))

def periodic_retrieve():
    print("Periodic called")

    
    response = requests.get("https://www.cryptingup.com/api/exchanges")
    exchanges = response.json()["exchanges"]
    for exchange in exchanges:    
        insert_into_exchanges(exchange["exchange_id"], exchange["name"], exchange["website"], exchange["volume_24h"])
      
    response = requests.get("https://www.cryptingup.com/api/assets/USD/markets?size=5000")
    markets = response.json()["markets"]
    for market in markets:    
        insert_into_markets(market["exchange_id"], market["symbol"], market["base_asset"], market["quote_asset"],
                       market["price_unconverted"], market["price"], market["change_24h"], market["spread"],
                       market["volume_24h"], market["status"], market["created_at"], market["updated_at"])
    response = requests.get("https://www.cryptingup.com/api/assets?size=2000")
    assets = response.json()["assets"]
    for asset in assets:    
        insert_into_assets(asset["asset_id"], asset["name"], asset["price"], asset["volume_24h"],
                       asset["change_1h"], asset["change_24h"], asset["change_7d"], asset["status"],
                       asset["created_at"], asset["updated_at"])
   
    threading.Timer(120, periodic_retrieve).start()
    
connection = mysql.connector.connect(user='root', password='Buzgon1344', host='127.0.0.1', database='kriptodatabase')
cursor = connection.cursor()
periodic_retrieve()
#if connection.is_connected():
#    cursor.close()
#    connection.close()
#    print("MySQL connection is closed")
