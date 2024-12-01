from pymongo import MongoClient
from faker import Faker
import random
import pendulum

# run: python main.py

# เชื่อมต่อ MongoDB
client = MongoClient("mongodb://mongo:mg1234@localhost:27017/")
db = client["retail"]
product_col = db["product_attributes"] # collection ~ table in sql

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

product = generate_product()
product_col.insert_one(product)
print(f"เพิ่มสินค้า: {product['name']}")
