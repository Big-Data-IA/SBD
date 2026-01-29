#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""generar_y_subir_s3_comercio360.py

Genera en el momento (datos sintéticos) las tablas del caso Comercio360 y las sube a S3.

- No requiere guardar CSV en disco (sube los ficheros desde memoria).
- Requiere boto3 y credenciales AWS disponibles en el entorno (AWS Academy normalmente las provee).

Tablas (CSV):
  - customers.csv
  - products.csv
  - stores.csv
  - orders.csv
  - order_items.csv

Ejemplo de uso:
  python generar_y_subir_s3_comercio360.py \
    --bucket MI_BUCKET --prefix comercio360/alumno_raul \
    --seed 123 --customers 500 --products 150 --stores 10 --orders 4000 --max-items 6

Salida en S3:
  s3://MI_BUCKET/comercio360/alumno_raul/customers.csv  (y resto)

NOTA: Los datos son sintéticos y ficticios (no datos reales).
"""

import argparse
import csv
import io
import random
import string
from datetime import date, timedelta


CITIES = [
    "Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza", "Málaga", "Murcia",
    "Palma", "Bilbao", "Alicante", "Córdoba", "Valladolid", "Vigo", "Gijón", "Granada",
]

CATEGORIES = [
    "Electrónica", "Hogar", "Moda", "Deporte", "Juguetes", "Alimentación", "Libros", "Belleza"
]

PAYMENT_METHODS = ["card", "cash", "transfer", "bizum"]


def _rand_str(rng, n=10, alphabet=string.ascii_letters + string.digits):
    return ''.join(rng.choice(alphabet) for _ in range(n))


def _choice_weighted(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def _rand_date(rng, start: date, end: date):
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, max(delta, 0)))


def generate_customers(rng, n_customers: int):
    rows = []
    for cid in range(1, n_customers + 1):
        city = rng.choice(CITIES)
        email = f"user{cid}_{_rand_str(rng, 6).lower()}@example.com"
        signup = _rand_date(rng, date.today() - timedelta(days=365 * 2), date.today())
        rows.append({
            "customer_id": cid,
            "email": email,
            "city": city,
            "signup_date": signup.isoformat(),
        })
    return rows


def generate_products(rng, n_products: int):
    base_price = {
        "Electrónica": 120,
        "Hogar": 45,
        "Moda": 35,
        "Deporte": 55,
        "Juguetes": 25,
        "Alimentación": 8,
        "Libros": 18,
        "Belleza": 22,
    }
    rows = []
    for pid in range(1, n_products + 1):
        cat = rng.choice(CATEGORIES)
        base = base_price[cat]
        price = max(1.0, rng.gauss(base, base * 0.25))
        rows.append({
            "product_id": pid,
            "product_name": f"{cat[:3].upper()}-{_rand_str(rng, 8).upper()}",
            "category": cat,
            "list_price": round(price, 2),
        })
    return rows


def generate_stores(rng, n_stores: int):
    rows = []
    for sid in range(1, n_stores + 1):
        city = rng.choice(CITIES)
        rows.append({
            "store_id": sid,
            "store_name": f"Tienda_{city}_{sid}",
            "city": city,
        })
    return rows


def generate_orders_and_items(rng, n_orders: int, customers, stores, products, max_items: int, days_back: int):
    cat_weights = {
        "Electrónica": 0.10,
        "Hogar": 0.12,
        "Moda": 0.18,
        "Deporte": 0.10,
        "Juguetes": 0.08,
        "Alimentación": 0.24,
        "Libros": 0.10,
        "Belleza": 0.08,
    }
    cats = list(cat_weights.keys())
    weights = [cat_weights[c] for c in cats]

    today = date.today()
    start = today - timedelta(days=days_back)

    orders = []
    items = []
    item_id = 1

    # index por categoría para acelerar
    products_by_cat = {}
    for p in products:
        products_by_cat.setdefault(p["category"], []).append(p)

    for oid in range(1, n_orders + 1):
        cust = rng.choice(customers)
        store = rng.choice(stores)
        odate = _rand_date(rng, start, today)
        pay = rng.choice(PAYMENT_METHODS)

        orders.append({
            "order_id": oid,
            "customer_id": cust["customer_id"],
            "store_id": store["store_id"],
            "order_date": odate.isoformat(),
            "payment_method": pay,
        })

        n_items = rng.randint(1, max_items)
        for _ in range(n_items):
            cat = _choice_weighted(rng, cats, weights)
            candidates = products_by_cat.get(cat) or products
            prod = rng.choice(candidates)

            qty = max(1, int(round(rng.gauss(2.0, 1.2))))
            discount = max(0.0, min(0.35, rng.random() * 0.25))
            unit_price = prod["list_price"] * (1.0 - discount)

            items.append({
                "order_item_id": item_id,
                "order_id": oid,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": round(unit_price, 2),
                "discount": round(discount, 3),
            })
            item_id += 1

    return orders, items


def csv_bytes(rows, fieldnames):
    s = io.StringIO()
    w = csv.DictWriter(s, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return s.getvalue().encode("utf-8")


def upload_to_s3(bucket: str, key: str, data: bytes):
    try:
        import boto3
    except Exception as e:
        raise RuntimeError("boto3 no está instalado. Instálalo con: pip install boto3") from e

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="text/csv")


def main():
    ap = argparse.ArgumentParser(description="Genera datos Comercio360 y los sube a S3 (en memoria)")
    ap.add_argument("--bucket", required=True, help="Bucket S3 destino")
    ap.add_argument("--prefix", required=True, help="Prefijo S3 (carpeta), p.ej. comercio360/alumno_x")
    ap.add_argument("--seed", type=int, default=42, help="Semilla para generar datos únicos")

    ap.add_argument("--customers", type=int, default=500)
    ap.add_argument("--products", type=int, default=150)
    ap.add_argument("--stores", type=int, default=10)
    ap.add_argument("--orders", type=int, default=4000)
    ap.add_argument("--max-items", type=int, default=6)
    ap.add_argument("--days-back", type=int, default=120)

    args = ap.parse_args()

    rng = random.Random(args.seed)

    customers = generate_customers(rng, args.customers)
    products = generate_products(rng, args.products)
    stores = generate_stores(rng, args.stores)
    orders, order_items = generate_orders_and_items(
        rng, args.orders, customers, stores, products, args.max_items, args.days_back
    )

    prefix = args.prefix.strip("/")

    objects = {
        "customers.csv": (customers, ["customer_id", "email", "city", "signup_date"]),
        "products.csv": (products, ["product_id", "product_name", "category", "list_price"]),
        "stores.csv": (stores, ["store_id", "store_name", "city"]),
        "orders.csv": (orders, ["order_id", "customer_id", "store_id", "order_date", "payment_method"]),
        "order_items.csv": (order_items, ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"]),
    }

    for fname, (rows, fields) in objects.items():
        key = f"{prefix}/{fname}" if prefix else fname
        data = csv_bytes(rows, fields)
        upload_to_s3(args.bucket, key, data)
        print(f"[OK] Subido a s3://{args.bucket}/{key} ({len(rows)} filas)")


if __name__ == "__main__":
    main()
