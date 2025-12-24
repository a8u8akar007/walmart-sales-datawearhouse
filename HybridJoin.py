import threading
import time
import pymysql
import pandas as pd
from datetime import timedelta
import calendar

############## Connection Setup ###############

# Connection configuration
MYSQL_CONF = {
    "host": "",
    "port": 3306,
    "user": "",
    "password": "",
    "database": "",  
    "autocommit": True
}

## Func to set connection param
def set_connection_param(host, port, user, password):
    MYSQL_CONF["host"] = host
    MYSQL_CONF["port"] = port
    MYSQL_CONF["user"] = user
    MYSQL_CONF["password"] = password

#### function to connect db 
def db_conn(database=None):
    return pymysql.connect(
        host=MYSQL_CONF["host"],
        port=MYSQL_CONF["port"],
        user=MYSQL_CONF["user"],
        password=MYSQL_CONF["password"],
        database=database if database else MYSQL_CONF["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

######## Ask for MySQL connection details ##########

host = input("Enter MySQL host (default: localhost): ") or "localhost"
port_input = input("Enter MySQL port (default: 3306): ") or "3306"
port = int(port_input)
username = input("Enter MySQL username: ")
userpassword = input("Enter MySQL password: ")

set_connection_param(host, port, username, userpassword)


########### Ask for database name ############

db_name = input("Enter the database name to create/use: ")


conn = db_conn(database=None)
cursor = conn.cursor()

# Droping and Creating database
cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
cursor.execute(f"CREATE DATABASE {db_name};")
print(f"Database '{db_name}' is ready.")

# Switching  to the new database
MYSQL_CONF["database"] = db_name
conn.select_db(db_name)


sql_file = "datawearhouse.sql" 

with open(sql_file, 'r') as f:
    sql_commands = f.read()

for command in sql_commands.split(';'):
    cmd = command.strip()
    if cmd:
        cursor.execute(cmd)
print("Star Schema tables created successfully.")


print("Database and schema setup complete. Ready to start HYBRIDJOIN.")


##### SQL CONNECTION COMPLETED ######



###############################################################



########### LOADING DATA FROM CSV TO SQL ###########

customer = pd.read_csv("customer_master_data.csv")
product = pd.read_csv("product_master_data.csv")
transactions = pd.read_csv("transactional_data.csv")

# Func to find season from month number
def seasons(month):
    if month in [3,4,5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'summer'
    elif month in [12, 1, 2]:
        return 'Winter'
    else: 
        return 'Autumn'
        

#### Customer Master Data Loading
for _, row in customer.iterrows():
    cursor.execute(""" insert ignore into customer(customerid, gender, age_range, occupation, city_category, stay_years, martial_status ) 
                       Values(%s, %s, %s, %s, %s, %s, %s)""",

                       (row.Customer_ID, row.Gender, row.Age, row.Occupation, row.City_Category, row.Stay_In_Current_City_Years, row.Marital_Status)
                  )
conn.commit()
print("Customer Master Data loaded")

#### Supplier Master Data Loading
suppliers = product[['supplierID', 'supplierName']].drop_duplicates()


for _, row in suppliers.iterrows():
    cursor.execute(""" insert ignore into supplier(supplierid, supplierName) 
                       Values(%s, %s)""",

                       (row.supplierID, row.supplierName)
                  )
conn.commit()
print("Supplier Data loaded")

#### Store Master Data Loading
stores = product[['storeID', 'storeName']].drop_duplicates()


for _, row in stores.iterrows():
    cursor.execute(""" insert ignore into store(storeid, storeName) 
                       Values(%s, %s)""",

                       (row.storeID, row.storeName)
                  )
conn.commit()
print("stores Data loaded")

#### Product Master Data Loading
for _, row in product.iterrows():
    cursor.execute(""" insert ignore into product(productid, productCategory, price, supplierid, storeid) 
                       Values(%s, %s, %s, %s, %s)""",

                       (row.Product_ID, row.Product_Category, row['price$'], row.supplierID, row.storeID)
                  )
conn.commit()
print("Product Data loaded")

#### Date Master Data Loading
## Finding trnasactional data start and end date to populate th edate dim according to it



transactions['date'] = pd.to_datetime(transactions['date'], format='%Y-%m-%d')

min_date = transactions['date'].min()
max_date = transactions['date'].max()

print("Min date:", min_date)
print("Max date:", max_date)

start_date = min_date - timedelta(days=365)
end_date = max_date + timedelta(days=365)

print("Start date:", start_date)
print("End date:", end_date)

### Transforming and loading data with attributes like weekday, weeekend flag, quarter, season etc

current_date = start_date
rows = []

while current_date <= end_date:
    
    weekday = calendar.day_name[current_date.weekday()]  
    weekend_flag = 1 if weekday in ['Saturday', 'Sunday'] else 0
    quarter = (current_date.month - 1)//3 + 1
    season = seasons(current_date.month)
    half = 'H1' if current_date.month <= 6 else 'H2'

    rows.append((
        current_date.date(),     
        current_date.day,
        current_date.month,
        quarter,
        current_date.year,
        weekday,
        weekend_flag,
        season,
        half
    ))
    current_date += timedelta(days=1)

insert_query = """
INSERT IGNORE INTO dateDim
(date, day, month, quarter, year, weekday, weekend_flag, season, half_year)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

cursor.executemany(insert_query, rows)
conn.commit()
print("dim_date populated successfully!")

cursor.close()
conn.close()


###############  HYBRID JOIN  ############### 


#### initializing Varibales

STREAM_CSV = "transactional_data.csv"
STREAM_BUFFER_SIZE = 500000
HASH_SIZE = 10000
DISK_BUFFER_SIZE = 500
STREAM_DELAY = 0
FACT_BATCH_SIZE = 100000

stream_buffer = []
w = HASH_SIZE
stop_event = threading.Event()
hash_lock = threading.Lock()
date_cache = {}        
date_lock = threading.Lock()
fact_batch = []
total_transactions = 0
hash_inserts = 0
hash_removes = 0

########### Doubly Queue  #############  

class QueueNode:
    def __init__(self, customer_id):
        self.customer_id = customer_id
        self.prev = None
        self.next = None

class DoublyLinkedQueue:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, customer_id):
        node = QueueNode(customer_id)
        if not self.head:
            self.head = self.tail = node
        else:
            node.prev = self.tail
            self.tail.next = node
            self.tail = node
        return node

    def popleft(self):
        if not self.head:
            return None
        node = self.head
        self.head = node.next
        if self.head:
            self.head.prev = None
        else:
            self.tail = None
        return node.customer_id

    def remove(self, node):
        if not node:
            return
        if node.prev:
            node.prev.next = node.next
        else:
            self.head = node.next
        if node.next:
            node.next.prev = node.prev
        else:
            self.tail = node.prev

    def peek(self):
        return self.head.customer_id if self.head else None

    def is_empty(self):
        return self.head is None


############## Hash Table Class #####################

class HashTable:
    def __init__(self, size=HASH_SIZE):
        self.size = size
        self.buckets = [[] for _ in range(size)]

    def _hash(self, key):
        return key % self.size

    def insert(self, customer_id, transaction, queue_node):
        idx = self._hash(customer_id)
        self.buckets[idx].append((transaction, queue_node))

    def get_and_remove(self, customer_id):
        idx = self._hash(customer_id)
        matched = []
        remaining = []
        for t, qnode in self.buckets[idx]:
            if t["Customer_ID"] == customer_id:
                matched.append((t, qnode))
            else:
                remaining.append((t, qnode))
        self.buckets[idx] = remaining
        return matched

    def occupancy(self):
        return sum(len(bucket) for bucket in self.buckets)

############## Product Cache ##############

#### Loading the product tabel in to the memory for fast lookup
def load_product_cache():
    
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM product p join store s on p.storeid = s.storeid join supplier sup on p.supplierid = sup.supplierid")
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    conn.close()

    cache = {}
    for _, r in df.iterrows():
        pid = str(r["productid"]).strip().upper()
        cache[pid] = {
            "Product_Category": r["productCategory"],
            "price": r["price"],
            "supplierid": r["supplierid"],
            "storeid": r["storeid"],
            "suppliername": r["supplierName"],
            "storename" : r["storeName"]
        }
    return cache

product_cache = load_product_cache()

############### Customer Cache  #####################

#### Added customer data from disk to memory for fast look up
def load_customer_table():
    
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM customer ORDER BY customerid")
    rows = cur.fetchall()
    conn.close()

    sorted_customers = sorted(rows, key=lambda r: r["customerid"])

    customer_map = {row["customerid"]: row for row in sorted_customers}

    print("Customers loaded:", len(sorted_customers))
    return sorted_customers, customer_map


CUSTOMER_LIST, CUSTOMER_MAP = load_customer_table()


#### Function to find date id from full date
def get_date_id(raw_date_str):
    date_key = raw_date_str   

    
    with date_lock:
        if date_key in date_cache:
            return date_cache[date_key]

    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()

    cur.execute("SELECT dateid FROM dateDim WHERE date = %s", (date_key,))
    row = cur.fetchone()

    if row:
        date_id = row[0]
        with date_lock:
            date_cache[date_key] = date_id
        cur.close()
        conn.close()
        return date_id

    # Insert if not found
    cur.execute("INSERT INTO dateDim (date) VALUES (%s)", (date_key,))
    conn.commit()
    date_id = cur.lastrowid

    with date_lock:
        date_cache[date_key] = date_id

    cur.close()
    conn.close()
    return date_id


#### Date table for fast lookup
def load_date_dimension():
    global date_cache
    conn = pymysql.connect(**MYSQL_CONF)
    cur = conn.cursor()

    cur.execute("SELECT dateid, date FROM dim_date")
    rows = cur.fetchall()

    for row in rows:
        date_id = row[0]
        date_key = row[1].strftime("%Y-%m-%d")
        date_cache[date_key] = date_id

    cur.close()
    conn.close()
    print(f"[Date Dimension] Loaded {len(date_cache)} dates.")

#### Funtion to insert table into fact table

### Funtion to insert batch of rows into the fact table

def insert_fact_bulk(rows):
    if not rows:
        return
    conn = db_conn()
    with conn.cursor() as cur:
        sql = """
            INSERT IGNORE INTO fact_sales( orderid, date, quantity, customerid, productid, storeid, supplierid, dateid,
                Gender, Age, Occupation, City_Category, Stay_In_Current_City_Years, Marital_Status,
                Product_Category, price, StoreName, SupplierName,total_amount )
            VALUES (
                %(orderid)s, %(date)s, %(quantity)s, %(customerid)s, %(productid)s, %(storeid)s, %(supplierid)s, %(dateid)s,
                %(Gender)s, %(Age)s, %(Occupation)s, %(City_Category)s, %(Stay)s, %(Marital)s,
                %(Product_Category)s, %(price)s, %(StoreName)s, %(SupplierName)s, %(total_amount)s
            )
        """
        cur.executemany(sql, rows)
    conn.close()


################ Stream Feeder #################

def stream_feeder():

    df = pd.read_csv(STREAM_CSV)
    df = df.iloc[8000:] 
    df["RawDate"] = df["date"]
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    for _, r in df.iterrows():
        tup = {
            "orderID": int(r["orderID"]),
            "Customer_ID": int(r["Customer_ID"]),
            "Product_ID": str(r["Product_ID"]),
            "quantity": int(r["quantity"]),
            "RawDate": r["RawDate"],            
            "date": r["date"]
        }

       
        while len(stream_buffer) >= STREAM_BUFFER_SIZE:
            time.sleep(0)

        stream_buffer.append(tup)
        time.sleep(STREAM_DELAY)

    stop_event.set()


##################  Hybrid Join Working Function  #####################

hash_table = HashTable(HASH_SIZE)
queue_keys = DoublyLinkedQueue()
batch_count = 0       
joined_count = 0      


def hybrid_worker():
    global w,  total_transactions, hash_inserts, hash_removes, batch_count, joined_count

    conn = db_conn()

    df = pd.read_csv(STREAM_CSV)

    # Picking up the first 8000 rows before the start of join
    initial_rows = df.head(8000)

    for _, r in initial_rows.iterrows():
        tup = {
            "orderID": int(r["orderID"]),
            "Customer_ID": int(r["Customer_ID"]),
            "Product_ID": str(r["Product_ID"]),
            "quantity": int(r["quantity"]),
            "RawDate": r["date"],
            "date": pd.to_datetime(r["date"], format="%Y-%m-%d", errors="coerce")
        }
        
        # Insert into hash table and queue
        c = tup["Customer_ID"]
        queue_node = queue_keys.append(c)  
        hash_table.insert(c, tup, queue_node)   
        w -= 1                                     

    if w < 0:
        w = 0
        print("w is negative, reset to 0")


    while True:
        with hash_lock:
            done = stop_event.is_set() and not stream_buffer and hash_table.occupancy() == 0 and queue_keys.is_empty()
        if done:
            if fact_batch:
                insert_fact_bulk(fact_batch)
                fact_batch.clear()
            break

        # 1) Moving data from stream to hash table with hash func
        while w > 0 and stream_buffer:
            tup = stream_buffer.pop(0) 
            raw_date = tup["date"]      
            date_id  = get_date_id(raw_date)
            cust = tup["Customer_ID"]
            with hash_lock:
                queue_node = queue_keys.append(cust)
                hash_table.insert(cust, tup, queue_node)
                hash_inserts += 1
                w -= 1

        if hash_table.occupancy() == 0:
            time.sleep(0.001)
            continue

        # 2) Now getting the oldest queue to fetch data from the disk
        oldest_key = queue_keys.peek()
        if oldest_key is None:
            time.sleep(0.001)
            continue

        # Locate the index of oldest_key inside customer list 
        left, right = 0, len(CUSTOMER_LIST) - 1
        found_index = None

        while left <= right:
            mid = (left + right) // 2
            if CUSTOMER_LIST[mid]["customerid"] == oldest_key:
                found_index = mid
                break
            elif CUSTOMER_LIST[mid]["customerid"] < oldest_key:
                left = mid + 1
            else:
                right = mid - 1

        # If key not found, skip
        if found_index is None:
            queue_keys.popleft()
            continue

        # ---- LOAD 500 CUSTOMER RECORDS ----
        start_index = max(0, found_index - 250)
        end_index = min(len(CUSTOMER_LIST), start_index + 500)

        disk_buffer = CUSTOMER_LIST[start_index:end_index]

        # Remove this key from queue
        queue_keys.popleft()

        # 4) Findind match in the hash table with the data imported from the disk
        
        for r in disk_buffer:
            cid = r["customerid"]
            with hash_lock:
                matched_trans = hash_table.get_and_remove(cid)
                hash_removes += len(matched_trans)
                total_transactions += len(matched_trans)
                w += len(matched_trans)  
                joined_count += len(matched_trans) 
            if not matched_trans:
                continue  

            for t, qnode in matched_trans:
                queue_keys.remove(qnode)

                prod_info = product_cache.get(t["Product_ID"], {})
                enriched = {
                    "orderid": t["orderID"],
                    "date": t["date"],
                    "quantity": t["quantity"],
                    "customerid": cid,
                    "productid": t["Product_ID"],
                    "storeid": prod_info.get("storeid"),
                    "supplierid": prod_info.get("supplierid"),
                    "Product_Category": prod_info.get("Product_Category"),
                    "price": prod_info.get("price"),
                    "Gender": r["gender"],
                    "Age": r["age_range"],
                    "Occupation": r["occupation"],
                    "City_Category": r["city_category"],
                    "Stay": r["stay_years"],
                    "Marital": r["martial_status"],
                    "StoreName": prod_info.get("storename"),
                    "SupplierName": prod_info.get("suppliername"),
                    "dateid": get_date_id( t["date"] ),
                    "total_amount": (prod_info.get("price", 0) * t["quantity"])
                }

                
                fact_batch.append(enriched)
                if len(fact_batch) >= FACT_BATCH_SIZE:
                    insert_fact_bulk(fact_batch)
                    batch_count += 1
                    fact_batch.clear()

        
        
        time.sleep(0.001)  

    conn.close()




start_time = time.time()
feeder = threading.Thread(target=stream_feeder)
worker = threading.Thread(target=hybrid_worker)

feeder.start()
worker.start()

feeder.join()
worker.join()


end_time = time.time()
elapsed = end_time - start_time

print("\n========== HYBRID JOIN REPORT ==========")
print(f"Total transactions processed: {total_transactions}")
print(f"Hash table inserts: {hash_inserts}")
print(f"Hash table removals: {hash_removes}")
print(f"Elapsed time: {elapsed:.2f} seconds")
if elapsed > 0:
    print(f"Throughput: {total_transactions / elapsed:.2f} rows/sec")
print("========================================\n")

print("\nHYBRIDJOIN PROCESS COMPLETE")

