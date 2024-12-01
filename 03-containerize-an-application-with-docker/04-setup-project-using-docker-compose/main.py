import time
import random
import pendulum
import pandas as pd
from faker import Faker
from pymongo import MongoClient
from sqlalchemy import create_engine

# run: python main.py

# เชื่อมต่อ MongoDB
# "mongodb://username:password@ip_or_name_in_docker:port"
client = MongoClient("mongodb://mongo:mg1234@mongodb:27017/")
db = client["retail"]
product_col = db["product_attributes"]

# สร้าง connection string สำหรับ PostgreSQL
# "postgresql;//username:password:ip_or_name_in_docker:port/db"
engine = create_engine("postgresql://postgres:pg1234@postgres:5432/postgres")

# สร้าง Faker object
fake = Faker()

# กำหนดโครงสร้างข้อมูลสินค้า
product_schema = {
    "name": str,  # ชื่อสินค้า
    "description": str,  # รายละเอียดสินค้า
    "price": float,  # ราคา
    "category": ["Electronics", "Clothing", "Books", "Home & Garden", "Sports"],
    "condition": ["New", "Used", "Refurbished"],
    "stock": int,  # จำนวนในคลัง
    "manufacturer": str,  # ผู้ผลิต
    "sku": str,  # รหัสสินค้า
    "created_at": "datetime",  # วันที่สร้าง
    "rating": float,  # คะแนน
    "reviews_count": int,  # จำนวนรีวิว
}


# สร้างและบันทึกข้อมูลสินค้าใน MongoDB
def generate_product():
    product = {
        "name": fake.catch_phrase(),
        "description": fake.text(),
        "price": round(random.uniform(10.0, 1000.0), 2),
        "category": random.choice(product_schema["category"]),
        "condition": random.choice(product_schema["condition"]),
        "stock": random.randint(0, 100),
        "manufacturer": fake.company(),
        "sku": fake.ean13(),
        "created_at": pendulum.now("Asia/Bangkok"),
        "rating": round(random.uniform(1, 5), 1),
        "reviews_count": random.randint(0, 1000),
    }
    return product


def generate_customer(num: int):
    # สร้างและบันทึกข้อมูลลูกค้าใน PostgreSQL
    customers = []
    for _ in range(num):
        customer = {
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address(),
            "segment": fake.random_element(
                ["Retail", "Wholesale", "Online", "In-Store"]
            ),
            "join_date": pendulum.now("Asia/Bangkok"),
            "loyalty_points": fake.random_int(min=0, max=1000),
        }
        customers.append(customer)
    return customers


while True:
    product = generate_product()
    product_col.insert_one(product)
    print(f"เพิ่มสินค้า: {product['name']}")

    customers = generate_customer(10)
    df = pd.DataFrame(customers)
    df.to_sql("customers", con=engine, if_exists="append", index=False)
    print(f"เพิ่มข้อมูลลูกค้าเรียบร้อยแล้ว")
    time.sleep(5)
