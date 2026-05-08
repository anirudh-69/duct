def extract_products(ctx, **kwargs):
    import time
    time.sleep(0.05)
    return [
        {"id": 1, "name": "Widget A", "price": 29.99},
        {"id": 2, "name": "Gadget B", "price": 49.99},
        {"id": 3, "name": "Tool C", "price": 19.99},
        {"id": 4, "name": "Device D", "price": 99.99},
    ]

def extract_customers(ctx, **kwargs):
    import time
    time.sleep(0.05)
    return [
        {"id": 1, "name": "Alice Smith", "tier": "gold"},
        {"id": 2, "name": "Bob Jones", "tier": "silver"},
        {"id": 3, "name": "Carol White", "tier": "gold"},
        {"id": 4, "name": "Dave Brown", "tier": "bronze"},
    ]

def extract_orders(ctx, **kwargs):
    import time
    time.sleep(0.05)
    return [
        {"id": 101, "customer_id": 1, "product_id": 1, "qty": 2, "total": 59.98},
        {"id": 102, "customer_id": 2, "product_id": 2, "qty": 1, "total": 49.99},
        {"id": 103, "customer_id": 1, "product_id": 3, "qty": 5, "total": 99.95},
        {"id": 104, "customer_id": 3, "product_id": 4, "qty": 1, "total": 99.99},
        {"id": 105, "customer_id": 4, "product_id": 1, "qty": 3, "total": 89.97},
    ]

def enrich_products(extract_products, extract_customers, ctx, **kwargs):
    return [p for p in extract_products if p["price"] > 20]

def enrich_customers(extract_customers, extract_orders, ctx, **kwargs):
    customer_order_count = {}
    for o in extract_orders:
        cid = o["customer_id"]
        customer_order_count[cid] = customer_order_count.get(cid, 0) + 1
    for c in extract_customers:
        c["order_count"] = customer_order_count.get(c["id"], 0)
    return extract_customers

def transform_orders(extract_orders, ctx, **kwargs):
    return [
        {
            "order_id": o["id"],
            "customer_id": o["customer_id"],
            "product_id": o["product_id"],
            "qty": o["qty"],
            "revenue": o["total"],
            "margin_pct": 30.0,
        }
        for o in extract_orders
    ]

def aggregate_revenue(enrich_products, transform_orders, ctx, **kwargs):
    product_revenue = {}
    for o in transform_orders:
        pid = o["product_id"]
        product_revenue[pid] = product_revenue.get(pid, 0) + o["revenue"]
    return [{"product_id": k, "total_revenue": round(v, 2)} for k, v in product_revenue.items()]

def merge_orders_product(transform_orders, enrich_products, ctx, **kwargs):
    product_map = {p["id"]: p for p in enrich_products}
    merged = []
    for o in transform_orders:
        p = product_map.get(o["product_id"])
        if p:
            merged.append({**o, "product_name": p["name"], "price": p["price"]})
    return merged

def quality_check(enrich_products, enrich_customers, ctx, **kwargs):
    issues = []
    if len(enrich_products) < 1:
        issues.append("No products found")
    if len(enrich_customers) < 1:
        issues.append("No customers found")
    if issues:
        raise ValueError(f"Quality issues: {issues}")
    return {"products": len(enrich_products), "customers": len(enrich_customers)}

def quality_aggregate(aggregate_revenue, ctx, **kwargs):
    total = sum(r["total_revenue"] for r in aggregate_revenue)
    return {"total_revenue": round(total, 2), "product_count": len(aggregate_revenue)}

def aggregate_by_customer(merge_orders_product, ctx, **kwargs):
    by_customer = {}
    for row in merge_orders_product:
        cid = row["customer_id"]
        if cid not in by_customer:
            by_customer[cid] = {"customer_id": cid, "revenue": 0.0, "order_count": 0}
        by_customer[cid]["revenue"] += row["revenue"]
        by_customer[cid]["order_count"] += 1
    return list(by_customer.values())

def lineage_check(merge_orders_product, ctx, **kwargs):
    if not merge_orders_product:
        raise ValueError("No orders to check")
    return {"rows": len(merge_orders_product)}

def prepare_final_dataset(aggregate_by_customer, quality_aggregate, ctx, **kwargs):
    return {
        "summary": quality_aggregate,
        "by_customer": aggregate_by_customer,
    }

def final_quality_check(prepare_final_dataset, lineage_check, ctx, **kwargs):
    if prepare_final_dataset["summary"]["total_revenue"] <= 0:
        raise ValueError("Invalid revenue")
    if lineage_check["rows"] <= 0:
        raise ValueError("Invalid row count")
    return {"status": "pass", "total_revenue": prepare_final_dataset["summary"]["total_revenue"]}

def export_report(ctx, prepare_final_dataset=None, **kwargs):
    import csv
    from pathlib import Path
    out = ctx.output_base / "final_report.csv"
    with open(out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["customer_id", "revenue", "order_count"])
        writer.writeheader()
        writer.writerows(prepare_final_dataset["by_customer"])
    print(f"Wrote {len(prepare_final_dataset['by_customer'])} rows to {out}")
    return str(out)