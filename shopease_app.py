# -*- coding: utf-8 -*-
"""
Run:  python shopease_app.py
"""

from flask import Flask, render_template_string, request, jsonify, Response, session
from datetime import datetime
import hashlib, json, time, copy, threading, queue

app = Flask(__name__)
app.secret_key = "shopease-secret-2024"

# ─── INITIAL DATA ────────────────────────────────────────────────────────────
DEFAULT_USERS = {
    "u001": {"user_id": "u001", "name": "Alice",   "email": "alice@shopease.com",   "city": "Mumbai", "avatar": "A", "color": "#E63946"},
    "u002": {"user_id": "u002", "name": "Bob",     "email": "bob@shopease.com",     "city": "Delhi",  "avatar": "B", "color": "#0F3460"},
    "u003": {"user_id": "u003", "name": "Charlie", "email": "charlie@shopease.com", "city": "Pune",   "avatar": "C", "color": "#2D6A4F"},
}

DEFAULT_PRODUCTS = {
    "p001": {"id": "p001", "name": "Wireless Headphones", "price": 1999,  "stock": 50,  "category": "Electronics", "icon": "🎧"},
    "p002": {"id": "p002", "name": "Running Shoes",        "price": 2499,  "stock": 30,  "category": "Footwear",     "icon": "👟"},
    "p003": {"id": "p003", "name": "Python Book",          "price": 799,   "stock": 100, "category": "Books",        "icon": "📘"},
    "p004": {"id": "p004", "name": "Smart Watch",          "price": 5999,  "stock": 8,   "category": "Electronics",  "icon": "⌚"},
    "p005": {"id": "p005", "name": "Mechanical Keyboard",  "price": 3499,  "stock": 20,  "category": "Electronics",  "icon": "⌨️"},
    "p006": {"id": "p006", "name": "Yoga Mat",             "price": 899,   "stock": 0,   "category": "Fitness",      "icon": "🧘"},
}

# Global shared state (in production this would be Redis/DB)
_state = {
    "products": copy.deepcopy(DEFAULT_PRODUCTS),
    "carts":    {uid: {} for uid in DEFAULT_USERS},
    "orders":   [],
    "payments": [],
    "logs":     [],
    "kafka":    [],
    "topic_counts": {
        "order-events": 0, "payment-events": 0, "notifications": 0,
        "product-events": 0, "analytics-events": 0, "DLQ": 0
    }
}
_state_lock = threading.Lock()

# SSE queues per session
_sse_queues = {}

def get_state():
    return _state

def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def fmt(n):
    return f"₹{n:,}"

def add_log(msg, kind="system"):
    with _state_lock:
        _state["logs"].append({"ts": ts(), "msg": msg, "kind": kind})

def add_kafka(topic, payload):
    with _state_lock:
        _state["kafka"].append({"ts": ts(), "topic": topic, "payload": payload})
        if topic in _state["topic_counts"]:
            _state["topic_counts"][topic] += 1

def push_sse(session_id, event_type, data):
    q = _sse_queues.get(session_id)
    if q:
        q.put({"event": event_type, "data": data})

def get_shard(key, n=3):
    h = int(hashlib.md5(str(key).encode()).hexdigest(), 16)
    return h % n

# ─── MICROSERVICES ────────────────────────────────────────────────────────────

def auth_validate(user_id):
    ok = user_id in DEFAULT_USERS
    add_log(f"Auth Service: Validating '{user_id}' → {'VALID ✓' if ok else 'INVALID ✗'}", "auth")
    return ok

def product_search(query, category=""):
    add_log(f"Search Service: Query='{query}' category='{category}'", "product")
    q = query.lower()
    results = []
    for p in _state["products"].values():
        match_q = not q or q in p["name"].lower() or q in p["category"].lower()
        match_c = not category or p["category"] == category
        if match_q and match_c:
            results.append(p)
    add_log(f"Search Service: Found {len(results)} result(s)", "product")
    return results

def check_stock(pid, qty):
    p = _state["products"].get(pid)
    return p and p["stock"] >= qty

def reduce_stock(pid, qty):
    if check_stock(pid, qty):
        _state["products"][pid]["stock"] -= qty
        name = _state["products"][pid]["name"]
        remaining = _state["products"][pid]["stock"]
        add_log(f"Product Service: Stock reduced — '{name}' → {remaining} remaining", "product")
        add_kafka("product-events", f"stock.reduced | product={pid} qty={qty} remaining={remaining}")
        return True
    add_log(f"Product Service: Insufficient stock for '{pid}'", "error")
    return False

def cart_add(user_id, pid, qty=1):
    add_log(f"Cart Service: Adding {qty}× '{pid}' for '{user_id}'", "cart")
    if not auth_validate(user_id):
        return {"ok": False, "msg": "User not found"}
    p = _state["products"].get(pid)
    if not p:
        return {"ok": False, "msg": "Product not found"}
    if p["stock"] < qty:
        return {"ok": False, "msg": f"Only {p['stock']} units in stock"}
    cart = _state["carts"].setdefault(user_id, {})
    cart[pid] = cart.get(pid, 0) + qty
    add_log(f"Cart Service: ✓ {p['name']} ×{cart[pid]} — Redis SET cart:{user_id} EX 1800", "cart")
    return {"ok": True}

def cart_remove(user_id, pid):
    cart = _state["carts"].get(user_id, {})
    if pid in cart:
        del cart[pid]
        add_log(f"Cart Service: Removed '{pid}' from cart for '{user_id}'", "cart")

def cart_clear(user_id):
    _state["carts"][user_id] = {}
    add_log(f"Cart Service: Cart cleared for '{user_id}' — Redis DEL cart:{user_id}", "cart")

def cart_display(user_id):
    raw = _state["carts"].get(user_id, {})
    items, total = {}, 0
    for pid, qty in raw.items():
        p = _state["products"].get(pid)
        if not p:
            continue
        sub = p["price"] * qty
        total += sub
        items[pid] = {**p, "quantity": qty, "subtotal": sub}
    return items, total

def payment_process(user_id, order_id, amount):
    add_log(f"Payment Service: Processing {fmt(amount)} for '{order_id}'", "payment")
    # Idempotency check
    for p in _state["payments"]:
        if p["order_id"] == order_id and p["status"] == "SUCCESS":
            add_log("Payment Service: Idempotency hit — already paid ✓", "payment")
            return True
    pay_id = f"pay_{len(_state['payments'])+1:04d}"
    _state["payments"].append({
        "payment_id": pay_id, "order_id": order_id,
        "user_id": user_id, "amount": amount,
        "status": "SUCCESS", "timestamp": ts()
    })
    add_log(f"Payment Service: {pay_id} — Razorpay SUCCESS ✓ (idempotency key stored)", "payment")
    add_kafka("payment-events", f"payment.success | order={order_id} amount={fmt(amount)} gateway=Razorpay")
    return True

def order_place(user_id, session_id=None):
    """8-step order placement with SSE pushes for live animation."""
    def push(step, status, msg):
        if session_id:
            push_sse(session_id, "pipeline", {"step": step, "status": status, "msg": msg})
        time.sleep(0.45)  # visual delay

    add_log(f"\n── Order Service: Starting placement for '{user_id}' ──", "order")

    # STEP 0 — Auth
    push(0, "active", "Validating user identity via Auth Service…")
    if not auth_validate(user_id):
        push(0, "fail", "User not found ✗")
        return None
    push(0, "done", "User validated ✓")

    # STEP 1 — Cart
    push(1, "active", "Fetching cart from Redis (TTL: 30 min)…")
    items, total = cart_display(user_id)
    if not items:
        add_log(f"Order Service: Cart is empty ✗", "error")
        push(1, "fail", "Cart is empty ✗")
        return None
    add_log(f"Cart Service: {len(items)} item(s), total {fmt(total)}", "cart")
    push(1, "done", f"{len(items)} item(s) found, subtotal {fmt(total)} ✓")

    # STEP 2 — Stock check
    push(2, "active", "Checking inventory for all items…")
    stock_ok = all(check_stock(pid, item["quantity"]) for pid, item in items.items())
    if not stock_ok:
        add_log("Order Service: Stock insufficient ✗", "error")
        push(2, "fail", "Insufficient stock ✗")
        return None
    push(2, "done", "All items in stock ✓")

    # STEP 3 — Create order
    push(3, "active", "Creating order record in PostgreSQL…")
    order_id = f"ORD_{len(_state['orders'])+1:04d}"
    shard = get_shard(order_id)
    order = {
        "order_id": order_id, "user_id": user_id,
        "items": {k: dict(v) for k, v in items.items()},
        "total": total, "status": "PENDING_PAYMENT",
        "timestamp": ts(), "shard": shard
    }
    _state["orders"].append(order)
    add_log(f"Order Service: '{order_id}' created → PENDING_PAYMENT (Shard {shard})", "order")
    add_kafka("order-events", f"order.placed | id={order_id} user={user_id} total={fmt(total)}")
    push(3, "done", f"Order {order_id} created → PENDING_PAYMENT (DB Shard {shard}) ✓")

    # STEP 4 — Payment
    push(4, "active", f"Calling Razorpay API — {fmt(total)}…")
    if not payment_process(user_id, order_id, total):
        order["status"] = "PAYMENT_FAILED"
        push(4, "fail", "Payment failed ✗")
        return None
    push(4, "done", f"Payment captured {fmt(total)} ✓")

    # STEP 5 — Reduce stock
    push(5, "active", "Deducting inventory from warehouse…")
    for pid, item in items.items():
        reduce_stock(pid, item["quantity"])
    add_kafka("product-events", f"inventory.updated | order={order_id} items={len(items)}")
    push(5, "done", "Inventory deducted ✓")

    # STEP 6 — Confirm
    push(6, "active", "Confirming order and publishing events to Kafka…")
    order["status"] = "CONFIRMED"
    add_log(f"Order Service: '{order_id}' → CONFIRMED ✓", "order")
    add_kafka("notifications", f"order.confirmed | order={order_id} → Push+SMS+Email to {DEFAULT_USERS[user_id]['name']}")
    add_kafka("analytics-events", f"order.completed | order={order_id} revenue={fmt(total)}")
    push(6, "done", "CONFIRMED — Kafka events published ✓")

    # STEP 7 — Clear cart
    push(7, "active", "Clearing cart from Redis…")
    cart_clear(user_id)
    add_log(f"Notification Service: Push sent → '{user_id}' — {order_id} confirmed! 🎉", "notif")
    push(7, "done", "Cart cleared — Notification sent ✓")

    # Final success event
    if session_id:
        push_sse(session_id, "order_complete", {
            "order_id": order_id,
            "total": fmt(total),
            "shard": shard
        })

    add_log(f"── Order Service: Complete — {order_id} ({fmt(total)}) ──\n", "order")
    return order_id

def order_cancel(order_id):
    for o in _state["orders"]:
        if o["order_id"] == order_id:
            if o["status"] == "CONFIRMED":
                o["status"] = "CANCELLED"
                add_log(f"Order Service: '{order_id}' cancelled ✓", "order")
                add_log(f"Payment Service: Refund initiated for '{order_id}' — 3–5 business days", "payment")
                add_kafka("payment-events", f"payment.refunded | order={order_id}")
                add_kafka("notifications", f"order.cancelled | order={order_id} → Refund email sent")
                return True
            add_log(f"Order Service: Cannot cancel — status is '{o['status']}' ✗", "error")
            return False
    add_log(f"Order Service: '{order_id}' not found ✗", "error")
    return False

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML_APP)

@app.route("/api/products")
def api_products():
    q = request.args.get("q", "")
    cat = request.args.get("cat", "")
    products = product_search(q, cat)
    # Attach cart quantities for current user
    uid = request.args.get("uid", "u001")
    cart = _state["carts"].get(uid, {})
    for p in products:
        p["in_cart"] = cart.get(p["id"], 0)
    return jsonify(products)

@app.route("/api/cart")
def api_cart():
    uid = request.args.get("uid", "u001")
    items, total = cart_display(uid)
    return jsonify({"items": list(items.values()), "total": total})

@app.route("/api/cart/add", methods=["POST"])
def api_cart_add():
    d = request.json
    result = cart_add(d["user_id"], d["product_id"], d.get("qty", 1))
    return jsonify(result)

@app.route("/api/cart/remove", methods=["POST"])
def api_cart_remove():
    d = request.json
    cart_remove(d["user_id"], d["product_id"])
    return jsonify({"ok": True})

@app.route("/api/cart/clear", methods=["POST"])
def api_cart_clear():
    d = request.json
    cart_clear(d["user_id"])
    return jsonify({"ok": True})

@app.route("/api/order/place", methods=["POST"])
def api_order_place():
    d = request.json
    uid = d["user_id"]
    sid = d.get("session_id")

    # Register SSE queue for this session
    if sid and sid not in _sse_queues:
        _sse_queues[sid] = queue.Queue()

    # Run order placement in background thread so SSE can stream
    def run():
        order_place(uid, sid)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Order processing started"})

@app.route("/api/order/cancel", methods=["POST"])
def api_order_cancel():
    d = request.json
    ok = order_cancel(d["order_id"])
    return jsonify({"ok": ok})

@app.route("/api/orders")
def api_orders():
    uid = request.args.get("uid")
    orders = [o for o in reversed(_state["orders"]) if not uid or o["user_id"] == uid]
    return jsonify(orders)

@app.route("/api/logs")
def api_logs():
    kind = request.args.get("kind", "")
    logs = _state["logs"]
    if kind:
        logs = [l for l in logs if l["kind"] == kind]
    return jsonify(list(reversed(logs[-200:])))

@app.route("/api/kafka")
def api_kafka():
    return jsonify({
        "events": list(reversed(_state["kafka"][-100:])),
        "counts": _state["topic_counts"]
    })

@app.route("/api/sharding")
def api_sharding():
    counts = [0, 0, 0]
    detail = [[], [], []]
    for o in _state["orders"]:
        s = get_shard(o["order_id"])
        counts[s] += 1
        detail[s].append(o["order_id"])
    demo = [0, 0, 0]
    for i in range(1, 31):
        oid = f"ORD_{i:04d}"
        demo[get_shard(oid)] += 1
    return jsonify({"real": counts, "demo": demo, "detail": detail, "total": len(_state["orders"])})

@app.route("/api/stats")
def api_stats():
    return jsonify({
        "users": len(DEFAULT_USERS),
        "products": len(_state["products"]),
        "orders": len(_state["orders"]),
        "payments": len(_state["payments"]),
        "kafka_events": len(_state["kafka"]),
        "cart_items": sum(sum(v.values()) for v in _state["carts"].values())
    })

@app.route("/api/reset", methods=["POST"])
def api_reset():
    with _state_lock:
        _state["products"] = copy.deepcopy(DEFAULT_PRODUCTS)
        _state["carts"]    = {uid: {} for uid in DEFAULT_USERS}
        _state["orders"]   = []
        _state["payments"] = []
        _state["logs"]     = []
        _state["kafka"]    = []
        _state["topic_counts"] = {k: 0 for k in _state["topic_counts"]}
    add_log("── Platform Reset ──", "system")
    return jsonify({"ok": True})

@app.route("/api/stream/<session_id>")
def api_stream(session_id):
    """Server-Sent Events stream for real-time pipeline animation."""
    if session_id not in _sse_queues:
        _sse_queues[session_id] = queue.Queue()

    def generate():
        q = _sse_queues[session_id]
        yield "data: {\"event\":\"connected\"}\n\n"
        while True:
            try:
                item = q.get(timeout=30)
                payload = json.dumps({"event": item["event"], "data": item["data"]})
                yield f"data: {payload}\n\n"
            except queue.Empty:
                yield "data: {\"event\":\"ping\"}\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ─── DIAGRAMS ROUTES ───────────────────────────────────────────────────────
import os

@app.route("/diagrams/shopease_architecture.xml")
def get_architecture_xml():
    with open("shopease_architecture.xml", "r") as f:
        return f.read(), 200, {
            "Content-Type": "application/xml",
            "Access-Control-Allow-Origin": "*"
        }

# ─── HTML TEMPLATE ─────────────────────────────────────────────────────────────
HTML_APP = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>ShopEase — E-Commerce System Design</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --ink:#0D0D0D; --ink-mid:#3D3D3D; --ink-soft:#888;
  --bg:#F5F4F0; --surface:#FFFFFF; --border:#E0DDD8; --border-dk:#C8C5BE;
  --primary:#1A1A2E; --accent:#E63946; --accent-lo:#fef0f1;
  --green:#2D6A4F; --green-lo:#d8f3dc;
  --amber:#B5451B; --amber-lo:#fff3cd;
  --blue:#0F3460; --blue-lo:#dde9f7;
  --purple:#4A0E8F; --purple-lo:#ede7f6;
  --mono:'Courier New',monospace; --sans:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  --r:10px; --r-lg:14px;
  --sh:0 2px 8px rgba(0,0,0,.08); --sh-lg:0 8px 32px rgba(0,0,0,.12);
}
html{font-size:14px;scroll-behavior:smooth}
body{font-family:var(--sans);background:var(--bg);color:var(--ink);min-height:100vh;line-height:1.55}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--border-dk);border-radius:3px}

/* TOPBAR */
#topbar{position:fixed;top:0;left:0;right:0;z-index:200;height:52px;
  background:var(--primary);display:flex;align-items:center;gap:14px;padding:0 20px;
  box-shadow:0 2px 16px rgba(0,0,0,.3);}
.logo{font-size:1.2rem;font-weight:800;color:#fff;letter-spacing:-0.5px;display:flex;align-items:center;gap:8px}
.logo-badge{background:var(--accent);color:#fff;font-size:.65rem;font-weight:700;padding:1px 6px;border-radius:4px;letter-spacing:.5px}
.tagline{font-size:.72rem;color:rgba(255,255,255,.45);flex:1}
.user-selector{display:flex;align-items:center;gap:8px;background:rgba(255,255,255,.1);border-radius:20px;padding:3px 12px 3px 4px}
.user-avatar{width:26px;height:26px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;color:#fff}
.user-select-native{background:transparent;border:none;color:#fff;font-size:.8rem;font-weight:600;cursor:pointer;outline:none}
.user-select-native option{color:var(--ink);background:var(--surface)}
.top-stats{display:flex;gap:16px}
.top-stat{font-size:.72rem;color:rgba(255,255,255,.6);display:flex;align-items:center;gap:4px}
.top-stat strong{color:#fff;font-size:.82rem}

/* LAYOUT */
#app{display:flex;height:calc(100vh - 52px);margin-top:52px}
#sidebar{width:200px;min-width:200px;background:var(--surface);border-right:1px solid var(--border);
  display:flex;flex-direction:column;overflow-y:auto;padding:10px 0}
#main{flex:1;overflow-y:auto}

/* NAV */
.nav-group{padding:8px 10px 2px;font-size:.64rem;font-weight:700;letter-spacing:1.1px;color:var(--ink-soft);text-transform:uppercase}
.nav-item{display:flex;align-items:center;gap:9px;padding:8px 14px;margin:1px 6px;border-radius:8px;
  cursor:pointer;font-size:.83rem;color:var(--ink-mid);font-weight:500;transition:.12s;user-select:none}
.nav-item:hover{background:var(--bg);color:var(--ink)}
.nav-item.active{background:var(--primary);color:#fff}
.nav-icon{width:18px;text-align:center;font-size:.95rem}
.nav-badge{margin-left:auto;background:var(--accent);color:#fff;border-radius:10px;
  padding:0 6px;font-size:.65rem;font-weight:700;min-width:16px;text-align:center;line-height:16px;height:16px}
.nav-badge.g{background:var(--green)}
.nav-div{height:1px;background:var(--border);margin:6px 12px}

/* PAGE */
.page{display:none;padding:24px 28px;min-height:100%}
.page.active{display:block}
.ph{margin-bottom:20px;display:flex;justify-content:space-between;align-items:flex-start}
.pt{font-size:1.5rem;font-weight:800;color:var(--primary);letter-spacing:-.5px}
.ps{font-size:.8rem;color:var(--ink-soft);margin-top:3px}

/* CARDS & GRIDS */
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);padding:18px;box-shadow:var(--sh)}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.g3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}
.g4{display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:12px}
.flex{display:flex;align-items:center;gap:10px}
.fb{display:flex;align-items:center;justify-content:space-between}

/* BUTTONS */
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border-radius:8px;border:none;
  font-size:.82rem;font-weight:600;cursor:pointer;transition:all .13s;white-space:nowrap;font-family:var(--sans)}
.btn:active{transform:scale(.97)}
.btn-p{background:var(--primary);color:#fff}
.btn-p:hover{background:#2a2a4e}
.btn-a{background:var(--accent);color:#fff}
.btn-a:hover{background:#c0303c}
.btn-g{background:var(--green);color:#fff}
.btn-g:hover{background:#245a42}
.btn-o{background:transparent;color:var(--ink-mid);border:1px solid var(--border)}
.btn-o:hover{background:var(--bg);border-color:var(--border-dk)}
.btn-sm{padding:5px 11px;font-size:.76rem}
.btn-xs{padding:3px 8px;font-size:.7rem}
.btn:disabled{opacity:.38;cursor:not-allowed;transform:none !important}
.btn-full{width:100%;justify-content:center}

/* INPUTS */
.inp{width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:8px;
  font-size:.83rem;color:var(--ink);background:var(--surface);outline:none;font-family:var(--sans);transition:.13s}
.inp:focus{border-color:var(--primary);box-shadow:0 0 0 3px rgba(26,26,46,.07)}
.sel{padding:7px 11px;border:1px solid var(--border);border-radius:8px;font-size:.82rem;
  color:var(--ink);background:var(--surface);cursor:pointer;outline:none}
.sel:focus{border-color:var(--primary)}
.lbl{font-size:.73rem;font-weight:600;color:var(--ink-mid);margin-bottom:3px;display:block}
input[type=number]{width:60px;padding:5px 8px;border:1px solid var(--border);border-radius:7px;
  font-size:.82rem;color:var(--ink);background:var(--surface);outline:none;text-align:center}
input[type=number]:focus{border-color:var(--primary)}

/* CATALOG */
.prod-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.prod-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);padding:16px;
  display:flex;gap:14px;transition:.15s;cursor:default}
.prod-card:hover{box-shadow:var(--sh);border-color:var(--border-dk)}
.prod-icon{width:48px;height:48px;border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:1.4rem;flex-shrink:0}
.prod-name{font-weight:700;font-size:.9rem;color:var(--ink)}
.prod-cat{font-size:.72rem;color:var(--ink-soft);margin-top:1px}
.prod-price{font-size:1.1rem;font-weight:800;color:var(--primary);margin-top:5px}
.prod-row{display:flex;align-items:center;gap:8px;margin-top:9px;flex-wrap:wrap}
.stk{display:inline-flex;align-items:center;gap:3px;font-size:.7rem;font-weight:600;padding:2px 7px;border-radius:5px}
.stk-in{background:var(--green-lo);color:var(--green)}
.stk-lo{background:var(--amber-lo);color:var(--amber)}
.stk-out{background:#fce8e8;color:var(--accent)}
.in-cart-badge{background:var(--blue-lo);color:var(--blue);font-size:.7rem;font-weight:700;padding:2px 7px;border-radius:5px}

/* CART */
.cart-item{display:flex;align-items:center;gap:12px;padding:12px;background:var(--surface);
  border:1px solid var(--border);border-radius:var(--r);margin-bottom:8px;transition:.15s}
.cart-item:hover{border-color:var(--border-dk)}
.ci-icon{width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1.1rem;flex-shrink:0}
.ci-name{font-weight:600;font-size:.88rem}
.ci-price{font-size:.75rem;color:var(--ink-soft);margin-top:1px}
.ci-sub{font-weight:700;color:var(--primary);margin-left:auto;font-size:.9rem}

/* ORDER CARDS */
.order-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r-lg);padding:16px;margin-bottom:10px}
.order-hdr{display:flex;align-items:center;gap:10px;margin-bottom:12px;flex-wrap:wrap}
.order-id{font-weight:700;font-family:var(--mono);color:var(--primary);font-size:.88rem}
.pill{display:inline-flex;align-items:center;font-size:.68rem;font-weight:700;padding:2px 9px;border-radius:9px;letter-spacing:.3px;text-transform:uppercase}
.pill-confirmed{background:var(--green-lo);color:var(--green)}
.pill-pending{background:var(--amber-lo);color:var(--amber)}
.pill-cancelled{background:#fce8e8;color:var(--accent)}
.pill-placed{background:var(--blue-lo);color:var(--blue)}
.order-items-tbl{font-size:.78rem;width:100%;border-collapse:collapse;margin-bottom:10px}
.order-items-tbl td{padding:4px 0;border-bottom:1px solid var(--border)}
.order-items-tbl tr:last-child td{border-bottom:none}
.order-total{font-size:.95rem;font-weight:700;color:var(--primary);text-align:right}

/* PIPELINE */
.pipeline-wrap{display:flex;align-items:center;gap:0;overflow-x:auto;padding:12px 0 16px}
.p-node{flex-shrink:0;background:var(--surface);border:2px solid var(--border);border-radius:10px;
  padding:10px 12px;text-align:center;min-width:100px;transition:.3s}
.p-node.active{border-color:var(--accent);box-shadow:0 0 0 3px rgba(230,57,70,.12);background:#fff8f8}
.p-node.done{border-color:var(--green);background:var(--green-lo)}
.p-node.fail{border-color:var(--accent);background:#fce8e8}
.p-node-icon{font-size:1.3rem;margin-bottom:4px}
.p-node-lbl{font-size:.67rem;font-weight:600;color:var(--ink-mid);line-height:1.3}
.p-node.active .p-node-lbl{color:var(--accent)}
.p-node.done .p-node-lbl{color:var(--green)}
.p-arrow{flex-shrink:0;height:2px;width:28px;background:var(--border);position:relative;transition:.3s}
.p-arrow.done{background:var(--green)}
.p-arrow::after{content:'';position:absolute;right:-5px;top:-4px;width:0;height:0;
  border-left:7px solid var(--border);border-top:5px solid transparent;border-bottom:5px solid transparent;transition:.3s}
.p-arrow.done::after{border-left-color:var(--green)}
.p-step-log{font-family:var(--mono);font-size:.73rem;line-height:1.9;color:#e6edf3;
  background:#0d1117;border-radius:var(--r);padding:12px;height:260px;overflow-y:auto}
.p-log-line{padding:0}
.p-log-ts{color:#484f58}
.p-log-step{color:#e3b341}
.p-log-msg{color:#e6edf3}
.p-log-ok{color:#3fb950}
.p-log-err{color:#f85149}

/* ORDER COMPLETE CARD */
#order-complete-card{display:none;background:var(--green-lo);border:2px solid var(--green);
  border-radius:var(--r-lg);padding:22px;text-align:center;margin-bottom:20px}
#order-complete-card.show{display:block;animation:popIn .35s ease}
@keyframes popIn{from{opacity:0;transform:scale(.94)}to{opacity:1;transform:scale(1)}}

/* KAFKA */
.kafka-feed{background:#0d1117;border-radius:var(--r);padding:12px;overflow-y:auto;font-family:var(--mono)}
.kf-row{display:flex;gap:10px;font-size:.7rem;padding:3px 0;border-bottom:1px solid rgba(255,255,255,.05)}
.kf-topic{font-weight:700;min-width:130px;flex-shrink:0}
.kf-payload{color:#e6edf3;opacity:.7;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.kf-ts{color:#484f58;flex-shrink:0}

/* SHARD BARS */
.sh-bar-wrap{margin-bottom:12px}
.sh-bar-lbl{display:flex;justify-content:space-between;font-size:.77rem;font-weight:600;margin-bottom:4px}
.sh-bar-track{height:20px;background:var(--bg);border-radius:5px;overflow:hidden;border:1px solid var(--border)}
.sh-bar-fill{height:100%;border-radius:5px;transition:width .6s ease;display:flex;align-items:center;padding-left:8px;font-size:.68rem;font-weight:700;color:#fff}

/* LOGS */
.log-console{background:#0d1117;border-radius:var(--r);padding:12px;overflow-y:auto;font-family:var(--mono);font-size:.72rem;line-height:1.8}
.log-row{display:flex;gap:8px}
.log-ts{color:#484f58;flex-shrink:0}
.log-auth{color:#58a6ff}.log-product{color:#3fb950}.log-cart{color:#f78166}
.log-payment{color:#e3b341}.log-order{color:#d2a8ff}.log-kafka{color:#79c0ff}
.log-notif{color:#ff7b72}.log-system{color:#8b949e}.log-error{color:#f85149}

/* ARCH */
.arch-layer{border:1px solid var(--border);border-radius:var(--r-lg);padding:14px 18px;margin-bottom:10px;cursor:pointer;transition:.15s}
.arch-layer:hover{border-color:var(--border-dk);box-shadow:var(--sh)}
.arch-layer.open{border-color:var(--primary);background:var(--bg)}
.arch-hdr{display:flex;align-items:center;gap:12px}
.arch-icon{width:34px;height:34px;border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:1.15rem;flex-shrink:0}
.arch-toggle{margin-left:auto;color:var(--ink-soft);transition:.2s;font-size:.9rem}
.arch-layer.open .arch-toggle{transform:rotate(90deg)}
.arch-body{display:none;margin-top:12px;padding-top:12px;border-top:1px solid var(--border)}
.arch-layer.open .arch-body{display:block}
.svc-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(175px,1fr));gap:8px;margin-top:10px}
.svc-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:9px 11px}
.svc-name{font-weight:700;font-size:.78rem;color:var(--primary)}
.svc-tech{font-size:.68rem;color:var(--ink-soft);margin-top:1px}
.svc-desc{font-size:.73rem;color:var(--ink-mid);margin-top:4px;line-height:1.4}

/* CODE */
.code-block{background:#0d1117;color:#e6edf3;border-radius:var(--r);padding:12px;font-family:var(--mono);font-size:.72rem;line-height:1.7;overflow-x:auto;margin:10px 0}
.kw{color:#ff7b72}.fn{color:#d2a8ff}.str{color:#a5d6ff}.cm{color:#8b949e}.num{color:#f2cc60}

/* MISC */
.chip{display:inline-block;font-size:.68rem;font-weight:600;padding:2px 8px;border-radius:5px}
.chip-b{background:var(--blue-lo);color:var(--blue)}.chip-g{background:var(--green-lo);color:var(--green)}
.chip-a{background:var(--amber-lo);color:var(--amber)}.chip-p{background:var(--purple-lo);color:var(--purple)}
.divider{height:1px;background:var(--border);margin:16px 0}
.empty{text-align:center;padding:36px 20px;color:var(--ink-soft)}
.empty-icon{font-size:2.2rem;margin-bottom:8px}
.empty p{font-size:.84rem}
.section-lbl{font-size:.72rem;font-weight:700;letter-spacing:.9px;text-transform:uppercase;color:var(--ink-soft);margin-bottom:10px}
.info-box{background:var(--blue-lo);border:1px solid #b8d4ef;border-radius:var(--r);padding:10px 14px;font-size:.8rem;color:var(--blue);margin-bottom:14px}
.metric-row{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:18px}
.metric{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:12px 16px;min-width:110px}
.metric-val{font-size:1.5rem;font-weight:800;color:var(--primary)}
.metric-lbl{font-size:.7rem;color:var(--ink-soft);margin-top:1px}

/* TOAST */
#toasts{position:fixed;bottom:20px;right:20px;z-index:999;display:flex;flex-direction:column;gap:7px}
.toast{background:var(--surface);border:1px solid var(--border);border-radius:9px;padding:10px 14px;
  box-shadow:var(--sh-lg);display:flex;align-items:center;gap:9px;min-width:250px;max-width:360px;animation:slideIn .22s ease}
.toast.success{border-left:4px solid var(--green)}.toast.error{border-left:4px solid var(--accent)}
.toast.info{border-left:4px solid var(--blue)}.toast.kafka{border-left:4px solid var(--purple)}
.toast-msg{font-size:.8rem;font-weight:500;color:var(--ink);flex:1}
@keyframes slideIn{from{opacity:0;transform:translateX(16px)}to{opacity:1;transform:translateX(0)}}

/* COMPARE TABLE */
.cmp-tbl{width:100%;border-collapse:collapse;font-size:.78rem}
.cmp-tbl th{background:var(--bg);font-weight:700;padding:7px 10px;text-align:left;border-bottom:2px solid var(--border)}
.cmp-tbl td{padding:7px 10px;border-bottom:1px solid var(--border)}
.cmp-tbl tr:last-child td{border-bottom:none}
</style>
</head>
<body>

<!-- TOPBAR -->
<div id="topbar">
  <div class="logo">ShopEase <span class="logo-badge">LIVE</span></div>
  <div class="top-stats" id="top-stats"></div>
  <div class="user-selector">
    <div class="user-avatar" id="top-av" style="background:#E63946">A</div>
    <select class="user-select-native" id="user-sel" onchange="switchUser(this.value)">
      <option value="u001">Alice · Mumbai</option>
      <option value="u002">Bob · Delhi</option>
      <option value="u003">Charlie · Pune</option>
    </select>
  </div>
  <button class="btn btn-sm" style="background:rgba(255,255,255,.12);color:#fff;border:none" onclick="resetAll()">↺ Reset</button>
</div>

<div id="app">
<!-- SIDEBAR -->
<div id="sidebar">
  <div class="nav-group">Store</div>
  <div class="nav-item active" onclick="nav('catalog')"><span class="nav-icon">🛍️</span>Catalog</div>
  <div class="nav-item" onclick="nav('cart')"><span class="nav-icon">🛒</span>Cart<span class="nav-badge" id="cart-badge" style="display:none">0</span></div>
  <div class="nav-item" onclick="nav('orders')"><span class="nav-icon">📦</span>Orders<span class="nav-badge g" id="order-badge" style="display:none">0</span></div>
  <div class="nav-div"></div>
  <div class="nav-group">System</div>
  <div class="nav-item" onclick="nav('pipeline')"><span class="nav-icon">⚡</span>Live Flow</div>
  <div class="nav-item" onclick="nav('kafka')"><span class="nav-icon">📨</span>Kafka Events</div>
  <div class="nav-item" onclick="nav('sharding')"><span class="nav-icon">🔀</span>DB Sharding</div>
  <div class="nav-item" onclick="nav('architecture')"><span class="nav-icon">🗺️</span>Architecture</div>
  <div class="nav-item" onclick="nav('logs')"><span class="nav-icon">🖥️</span>System Logs</div>
  <div class="nav-item" onclick="nav('diagrams')"><span class="nav-icon">📊</span>Diagrams</div>
  <div class="nav-div"></div>
  <div class="nav-group">Info</div>
  <div class="nav-item" onclick="nav('about')"><span class="nav-icon">📘</span>About</div>
</div>

<!-- MAIN -->
<div id="main">

<!-- CATALOG -->
<div class="page active" id="page-catalog">
  <div class="ph">
    <div><div class="pt">Product Catalog</div><div class="ps">Browse and add items to your cart</div></div>
    <div class="flex">
      <input class="inp" id="search-inp" placeholder="🔍 Search…" oninput="loadCatalog()" style="width:200px"/>
      <select class="sel" id="cat-sel" onchange="loadCatalog()">
        <option value="">All Categories</option>
        <option value="Electronics">Electronics</option>
        <option value="Footwear">Footwear</option>
        <option value="Books">Books</option>
        <option value="Fitness">Fitness</option>
      </select>
    </div>
  </div>
  <div class="prod-grid" id="prod-grid"></div>
</div>

<!-- CART -->
<div class="page" id="page-cart">
  <div class="ph"><div><div class="pt">Shopping Cart</div><div class="ps">Review your items and place order</div></div></div>
  <div class="g2" style="align-items:start;gap:20px">
    <div id="cart-list"></div>
    <div id="cart-summary"></div>
  </div>
</div>

<!-- ORDERS -->
<div class="page" id="page-orders">
  <div class="ph"><div><div class="pt">My Orders</div><div class="ps" id="orders-sub">Order history</div></div></div>
  <div id="orders-list"></div>
</div>

<!-- PIPELINE -->
<div class="page" id="page-pipeline">
  <div class="ph"><div><div class="pt">Live Order Flow</div><div class="ps">Watch your order execute step-by-step across all 8 microservices</div></div></div>

  <!-- ORDER COMPLETE CARD -->
  <div id="order-complete-card">
    <div style="font-size:2rem;margin-bottom:6px">🎉</div>
    <div style="font-weight:800;font-size:1.1rem;color:var(--green)">Order Confirmed!</div>
    <div id="oc-details" style="font-size:.85rem;color:var(--ink-mid);margin-top:4px"></div>
  </div>

  <div class="info-box" id="pipeline-hint">
    💡 Place an order from the Cart page — the pipeline animates here in real time showing each microservice step.
  </div>

  <div class="section-lbl">Order Execution Pipeline — 8 Steps</div>
  <div class="pipeline-wrap" id="pipeline-nodes"></div>

  <div class="g2" style="gap:16px;margin-top:16px">
    <div>
      <div class="section-lbl">Microservice Execution Log</div>
      <div class="p-step-log" id="pipeline-log" style="height:260px"></div>
    </div>
    <div>
      <div class="section-lbl">Kafka Events Published</div>
      <div class="kafka-feed" style="height:260px" id="pipeline-kafka"></div>
    </div>
  </div>
</div>

<!-- KAFKA -->
<div class="page" id="page-kafka">
  <div class="ph"><div><div class="pt">Kafka Event Stream</div><div class="ps">3 brokers · acks=all · replication factor 3 · idempotent=true</div></div>
  <button class="btn btn-o btn-sm" onclick="loadKafka()">↻ Refresh</button></div>
  <div class="metric-row" id="kafka-counts"></div>
  <div class="section-lbl">Live Event Feed</div>
  <div class="kafka-feed" id="kafka-feed" style="height:380px"></div>
  <div class="divider"></div>
  <div class="section-lbl">Topic Architecture</div>
  <div class="g3" id="kafka-topics"></div>
</div>

<!-- SHARDING -->
<div class="page" id="page-sharding">
  <div class="ph"><div><div class="pt">Database Sharding</div><div class="ps">Hash-based distribution across 3 PostgreSQL shards</div></div></div>
  <div class="g2" style="gap:20px;align-items:start">
    <div>
      <div class="card" style="margin-bottom:16px">
        <div class="section-lbl">Your Orders — Real Distribution</div>
        <div id="real-shards"></div>
      </div>
      <div class="card">
        <div class="section-lbl">Simulation — 30 Orders</div>
        <div id="demo-shards"></div>
        <div style="font-size:.73rem;color:var(--ink-soft);margin-top:6px">hash(order_id) mod 3 — even spread, no hotspots</div>
      </div>
    </div>
    <div>
      <div class="card" style="margin-bottom:16px">
        <div class="section-lbl">Strategy Comparison</div>
        <table class="cmp-tbl">
          <thead><tr><th>Shard Key</th><th>Hotspot?</th><th>Verdict</th></tr></thead>
          <tbody>
            <tr><td>user_id % 3</td><td>Power user → Shard 0 floods</td><td>❌ Bad</td></tr>
            <tr><td>category % 3</td><td>Electronics sale → one shard</td><td>❌ Bad</td></tr>
            <tr><td>hash(order_id) % 3</td><td>None — even spread</td><td>✅ Good</td></tr>
            <tr><td>Consistent Hash</td><td>~10% remap on scale-out</td><td>✅ Best</td></tr>
          </tbody>
        </table>
      </div>
      <div class="card">
        <div class="section-lbl">Hash Function (Python)</div>
        <div class="code-block"><span class="kw">import</span> hashlib<br><br><span class="kw">def</span> <span class="fn">get_shard</span>(key, num_shards=<span class="num">3</span>):<br>    <span class="cm"># MD5 → big int → mod N</span><br>    h = <span class="fn">int</span>(hashlib.md5(<br>        <span class="fn">str</span>(key).<span class="fn">encode</span>()<br>    ).hexdigest(), <span class="num">16</span>)<br>    <span class="kw">return</span> h % num_shards<br><br><span class="cm"># "ORD_0001" → Shard 2</span><br><span class="cm"># "ORD_0002" → Shard 0</span><br><span class="cm"># "ORD_0003" → Shard 1</span></div>
      </div>
    </div>
  </div>
</div>

<!-- ARCHITECTURE -->
<div class="page" id="page-architecture">
  <div class="ph"><div><div class="pt">System Architecture</div><div class="ps">Click each layer to expand microservice details</div></div></div>
  <div id="arch-layers"></div>
</div>

<!-- LOGS -->
<div class="page" id="page-logs">
  <div class="ph">
    <div><div class="pt">System Logs</div><div class="ps">Colour-coded by microservice</div></div>
    <div class="flex">
      <select class="sel" id="log-filter" onchange="loadLogs()">
        <option value="">All Services</option>
        <option value="auth">Auth Service</option>
        <option value="product">Product / Search</option>
        <option value="cart">Cart Service</option>
        <option value="payment">Payment Service</option>
        <option value="order">Order Service</option>
        <option value="kafka">Kafka</option>
        <option value="notif">Notifications</option>
      </select>
      <button class="btn btn-o btn-sm" onclick="loadLogs()">↻ Refresh</button>
    </div>
  </div>
  <div class="log-console" id="log-console" style="height:calc(100vh - 170px)"></div>
</div>

<!-- ABOUT -->
<div class="page" id="page-about">
  <div class="ph"><div><div class="pt">About This Demo</div><div class="ps">B.Tech CSE 2024-28 · System Design Assignment · Semester IV · ITM Skills University</div></div></div>
  <div class="g2" style="gap:18px">
    <div>
      <div class="card" style="margin-bottom:14px">
        <div class="section-lbl">Problem Statement</div>
        <p style="font-size:.82rem;color:var(--ink-mid);line-height:1.7">ShopEase is a large-scale e-commerce platform similar to Amazon. It must handle millions of users, a vast product catalog, fast search, secure payments, and reliable order processing — especially during flash sale spikes.</p>
      </div>
      <div class="card">
        <div class="section-lbl">Descriptive Questions Addressed</div>
        <div id="ab-questions"></div>
      </div>
    </div>
    <div>
      <div class="card" style="margin-bottom:14px">
        <div class="section-lbl">Python Services Simulated</div>
        <div id="ab-services"></div>
      </div>
      <div class="card">
        <div class="section-lbl">Production Tech Stack</div>
        <div id="ab-stack"></div>
      </div>
    </div>
  </div>
</div>

<!-- DIAGRAMS -->
<div class="page" id="page-diagrams">
  <div class="ph"><div><div class="pt">Architecture Diagrams</div><div class="ps">View diagrams using draw.io viewer</div></div></div>
  <div class="card">
    <div class="section-lbl">Diagram Viewer</div>
    <div id="diagram-container" style="height:70vh;border-radius:8px;overflow:hidden;border:1px solid var(--border);background:white;display:flex;align-items:center;justify-content:center">
      <p style="color:var(--ink-soft)">Loading diagram…</p>
    </div>
  </div>
</div>

</div><!-- #main -->
</div><!-- #app -->
<div id="toasts"></div>

<script>
// ─── STATE ──────────────────────────────────────────────────────────────────
const USERS = {
  u001:{name:"Alice", city:"Mumbai", avatar:"A", color:"#E63946"},
  u002:{name:"Bob",   city:"Delhi",  avatar:"B", color:"#0F3460"},
  u003:{name:"Charlie", city:"Pune", avatar:"C", color:"#2D6A4F"},
};
const PROD_COLORS = {
  Electronics:"#dde9f7", Footwear:"#d8f3dc", Books:"#fff3cd", Fitness:"#ede7f6"
};
let currentUser = "u001";
const sessionId = "sess_" + Math.random().toString(36).slice(2);
let pipelineKafkaBuffer = [];
let pipelineLogBuffer = [];

// ─── API ────────────────────────────────────────────────────────────────────
async function api(path, opts={}) {
  const r = await fetch(path, {
    method: opts.body ? "POST" : "GET",
    headers: {"Content-Type":"application/json"},
    body: opts.body ? JSON.stringify(opts.body) : undefined
  });
  return r.json();
}

// ─── NAV ────────────────────────────────────────────────────────────────────
function nav(page) {
  document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
  document.querySelectorAll(".nav-item").forEach(n => n.classList.remove("active"));
  document.getElementById("page-"+page).classList.add("active");
  document.querySelectorAll(".nav-item").forEach(n => {
    if(n.getAttribute("onclick") && n.getAttribute("onclick").includes(`'${page}'`)) n.classList.add("active");
  });
  const loaders = {
    catalog: loadCatalog, cart: loadCart, orders: loadOrders,
    kafka: loadKafka, sharding: loadSharding,
    architecture: renderArch, logs: loadLogs,
    about: renderAbout, pipeline: loadPipelineKafka,
    diagrams: () => loadDiagram('shopease_architecture')
  };
  if(loaders[page]) loaders[page]();
}

// ─── DIAGRAM LOADER ─────────────────────────────────────────────────────────
let _drawioMsgHandler = null;

function loadDiagram(name) {
  const label = 'System Architecture';
  const container = document.getElementById('diagram-container');
  if (_drawioMsgHandler) {
    window.removeEventListener('message', _drawioMsgHandler);
    _drawioMsgHandler = null;
  }
  container.style.display = 'flex';
  container.style.flexDirection = 'column';
  container.style.alignItems = 'stretch';
  container.style.justifyContent = 'stretch';
  container.innerHTML = `<div style="display:flex;flex-direction:column;width:100%;height:100%;min-height:0;">
    <div style="padding:10px;background:#f5f5f5;border-bottom:1px solid #ddd;display:flex;justify-content:space-between;align-items:center;flex-shrink:0;">
      <span style="font-weight:500;">${label}</span>
      <button class="btn btn-o btn-sm" onclick="openDiagramInNewTab('${name}')">🔗 Open in draw.io</button>
    </div>
    <div id="diagram-content" style="flex:1;width:100%;min-height:0;display:flex;align-items:center;justify-content:center;">
      <p>Loading diagram…</p>
    </div>
  </div>`;

  fetch(`/diagrams/${name}.xml`)
    .then(res => {
      if (!res.ok) throw new Error(`Could not load diagram (${res.status})`);
      return res.text();
    })
    .then(xml => embedDrawioDiagram(name, label, xml))
    .catch(err => {
      const el = document.getElementById('diagram-content');
      if (el) el.innerHTML = `<p style="color:red;padding:20px;">Error loading diagram: ${err.message}</p>`;
    });
}

function embedDrawioDiagram(name, label, xml) {
  const container = document.getElementById('diagram-container');
  container.innerHTML = `<div style="display:flex;flex-direction:column;width:100%;height:100%;min-height:0;">
    <div style="padding:10px;background:#f5f5f5;border-bottom:1px solid #ddd;display:flex;justify-content:space-between;align-items:center;flex-shrink:0;">
      <span style="font-weight:500;">${label}</span>
      <button class="btn btn-o btn-sm" onclick="openDiagramInNewTab('${name}')">🔗 Open in draw.io</button>
    </div>
    <iframe id="diagram-iframe" style="flex:1;width:100%;border:none;min-height:0;" title="${label}"></iframe>
  </div>`;

  const iframe = document.getElementById('diagram-iframe');
  let loaded = false;
  const sendXml = () => {
    if (loaded || !iframe.contentWindow) return;
    loaded = true;
    iframe.contentWindow.postMessage(JSON.stringify({
      action: 'load',
      xml: xml,
      autosave: 0
    }), '*');
  };

  _drawioMsgHandler = (evt) => {
    if (evt.source !== iframe.contentWindow) return;
    try {
      const msg = JSON.parse(evt.data);
      if (msg.event === 'init') sendXml();
    } catch (_) {}
  };
  window.addEventListener('message', _drawioMsgHandler);
  iframe.src = 'https://embed.diagrams.net/?embed=1&ui=min&spin=1&proto=json&noSaveBtn=1&noExitBtn=1';
}

function openDiagramInNewTab(name) {
  fetch(`/diagrams/${name}.xml`)
    .then(res => res.text())
    .then(xml => {
      const w = window.open('https://app.diagrams.net/?splash=0', '_blank');
      if (!w) return;
      const handler = (evt) => {
        if (evt.source !== w) return;
        try {
          const msg = JSON.parse(evt.data);
          if (msg.event === 'init') {
            w.postMessage(JSON.stringify({action: 'load', xml, autosave: 0}), '*');
            window.removeEventListener('message', handler);
          }
        } catch (_) {}
      };
      window.addEventListener('message', handler);
    });
}

// ─── USER ────────────────────────────────────────────────────────────────────
function switchUser(uid) {
  currentUser = uid;
  const u = USERS[uid];
  document.getElementById("top-av").textContent = u.avatar;
  document.getElementById("top-av").style.background = u.color;
  updateBadges();
  const active = document.querySelector(".page.active");
  if(active) {
    const pid = active.id.replace("page-","");
    nav(pid);
  }
}

// ─── BADGES ─────────────────────────────────────────────────────────────────
async function updateBadges() {
  const cart = await api(`/api/cart?uid=${currentUser}`);
  const cartN = cart.items.reduce((a,b)=>a+b.quantity, 0);
  const cb = document.getElementById("cart-badge");
  cb.style.display = cartN>0 ? "" : "none"; cb.textContent = cartN;

  const orders = await api(`/api/orders?uid=${currentUser}`);
  const confN = orders.filter(o=>o.status==="CONFIRMED").length;
  const ob = document.getElementById("order-badge");
  ob.style.display = confN>0 ? "" : "none"; ob.textContent = confN;

  loadTopStats();
}

async function loadTopStats() {
  const s = await api("/api/stats");
  document.getElementById("top-stats").innerHTML = `
    <div class="top-stat"><strong>${s.orders}</strong> orders</div>
    <div class="top-stat"><strong>${s.kafka_events}</strong> kafka events</div>
    <div class="top-stat"><strong>${s.payments}</strong> payments</div>
  `;
}

// ─── CATALOG ────────────────────────────────────────────────────────────────
async function loadCatalog() {
  const q = document.getElementById("search-inp").value.trim();
  const cat = document.getElementById("cat-sel").value;
  const products = await api(`/api/products?q=${encodeURIComponent(q)}&cat=${cat}&uid=${currentUser}`);
  const g = document.getElementById("prod-grid");
  if(!products.length){
    g.innerHTML=`<div class="empty" style="grid-column:1/-1"><div class="empty-icon">🔍</div><p>No products found</p></div>`;
    return;
  }
  g.innerHTML = products.map(p=>{
    const stockClass = p.stock===0?"stk-out":p.stock<5?"stk-lo":"stk-in";
    const stockLabel = p.stock===0?"✗ Out of stock":p.stock<5?`⚠ ${p.stock} left`:`✓ ${p.stock} in stock`;
    const bg = PROD_COLORS[p.category]||"#f0f0f0";
    return `<div class="prod-card">
      <div class="prod-icon" style="background:${bg}">${p.icon}</div>
      <div style="flex:1">
        <div class="prod-name">${p.name}</div>
        <div class="prod-cat">${p.category} · ID: ${p.id}</div>
        <div style="display:flex;align-items:center;gap:8px;margin-top:5px">
          <div class="prod-price">₹${p.price.toLocaleString('en-IN')}</div>
          <span class="stk ${stockClass}">${stockLabel}</span>
        </div>
        <div class="prod-row">
          <input type="number" id="qty-${p.id}" min="1" max="${Math.max(p.stock,1)}" value="1" ${p.stock===0?"disabled":""}>
          <button class="btn btn-p btn-sm" onclick="addToCart('${p.id}')" ${p.stock===0?"disabled":""}>
            🛒 Add${p.in_cart>0?` <span class="in-cart-badge">×${p.in_cart} in cart</span>`:""}
          </button>
        </div>
      </div>
    </div>`;
  }).join("");
}

async function addToCart(pid) {
  const qty = parseInt(document.getElementById(`qty-${pid}`).value)||1;
  const r = await api("/api/cart/add", {body:{user_id:currentUser, product_id:pid, qty}});
  if(r.ok){
    toast("Added to cart ✓", "success", "✅");
    loadCatalog();
    updateBadges();
  } else {
    toast(r.msg||"Failed to add", "error", "❌");
  }
}

// ─── CART ────────────────────────────────────────────────────────────────────
async function loadCart() {
  const {items, total} = await api(`/api/cart?uid=${currentUser}`);
  const list = document.getElementById("cart-list");
  const summary = document.getElementById("cart-summary");

  if(!items.length){
    list.innerHTML=`<div class="empty"><div class="empty-icon">🛒</div><p>Your cart is empty</p><button class="btn btn-p" style="margin-top:10px" onclick="nav('catalog')">Browse Catalog</button></div>`;
    summary.innerHTML=""; return;
  }

  const bg = PROD_COLORS;
  list.innerHTML = items.map(item=>`
    <div class="cart-item">
      <div class="ci-icon" style="background:${bg[item.category]||'#f0f0f0'}">${item.icon}</div>
      <div>
        <div class="ci-name">${item.name}</div>
        <div class="ci-price">₹${item.price.toLocaleString('en-IN')} × ${item.quantity}</div>
      </div>
      <div class="ci-sub">₹${item.subtotal.toLocaleString('en-IN')}</div>
      <button class="btn btn-o btn-xs" onclick="removeFromCart('${item.id}')">✕</button>
    </div>`).join("");

  const gst = Math.round(total*0.18);
  const delivery = total>500?0:49;
  const grand = total+gst+delivery;

  summary.innerHTML=`<div class="card">
    <div class="section-lbl">Order Summary</div>
    <div style="display:flex;justify-content:space-between;font-size:.83rem;padding:6px 0;border-bottom:1px solid var(--border)">
      <span>Subtotal (${items.length} item${items.length>1?"s":""})</span><span>₹${total.toLocaleString('en-IN')}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:.83rem;padding:6px 0;border-bottom:1px solid var(--border)">
      <span>GST (18%)</span><span>₹${gst.toLocaleString('en-IN')}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-size:.83rem;padding:6px 0;border-bottom:1px solid var(--border)">
      <span>Delivery</span><span>${delivery===0?'<span style="color:var(--green)">FREE</span>':"₹"+delivery}</span>
    </div>
    <div style="display:flex;justify-content:space-between;font-weight:800;font-size:1rem;padding:10px 0;color:var(--primary)">
      <span>Total</span><span>₹${grand.toLocaleString('en-IN')}</span>
    </div>
    <button class="btn btn-a btn-full" style="padding:12px;font-size:.9rem;margin-top:4px" onclick="placeOrder()">
      💳 Place Order — ₹${grand.toLocaleString('en-IN')}
    </button>
    <button class="btn btn-o btn-full btn-sm" style="margin-top:8px" onclick="clearCart()">🗑️ Clear Cart</button>
    <div style="margin-top:10px;font-size:.68rem;color:var(--ink-soft);text-align:center">
      🔒 Secured by Razorpay · Idempotency guaranteed · Retries 3× with backoff
    </div>
  </div>`;
}

async function removeFromCart(pid) {
  await api("/api/cart/remove", {body:{user_id:currentUser, product_id:pid}});
  toast("Item removed", "info", "🗑️");
  loadCart(); updateBadges();
}
async function clearCart() {
  await api("/api/cart/clear", {body:{user_id:currentUser}});
  toast("Cart cleared", "info", "🗑️");
  loadCart(); updateBadges();
}

async function placeOrder() {
  // Switch to pipeline page immediately to show animation
  nav("pipeline");
  resetPipeline();
  document.getElementById("pipeline-hint").style.display="none";
  document.getElementById("order-complete-card").classList.remove("show");
  pipelineLogBuffer = [];
  pipelineKafkaBuffer = [];
  renderPipelineLog();

  toast("Processing order…", "info", "⏳");

  // Start SSE listener then trigger order
  startSSE();
  await api("/api/order/place", {body:{user_id:currentUser, session_id:sessionId}});

  updateBadges();
  loadCart();
}

// ─── SSE PIPELINE ────────────────────────────────────────────────────────────
let sseSource = null;

function startSSE() {
  if(sseSource) { sseSource.close(); }
  sseSource = new EventSource(`/api/stream/${sessionId}`);
  sseSource.onmessage = (e) => {
    const raw = JSON.parse(e.data);
    const {event, data} = raw;

    if(event === "pipeline") {
      animateNode(data.step, data.status);
      const ts = new Date().toLocaleTimeString("en-IN",{hour12:false});
      const cls = data.status==="done"?"p-log-ok":data.status==="fail"?"p-log-err":"";
      pipelineLogBuffer.push(
        `<div class="p-log-line"><span class="p-log-ts">[${ts}]</span> ` +
        `<span class="p-log-step">Step ${data.step+1}/8</span> ` +
        `<span class="${cls}">${data.msg}</span></div>`
      );
      renderPipelineLog();

      if(data.status==="done" && data.step < 7) {
        document.getElementById(`parr-${data.step}`).classList.add("done");
      }
    }

    if(event === "order_complete") {
      const card = document.getElementById("order-complete-card");
      document.getElementById("oc-details").textContent =
        `Order ${data.order_id} · ${data.total} · DB Shard ${data.shard} · Kafka events published`;
      card.classList.add("show");
      toast(`🎉 Order ${data.order_id} confirmed!`, "success", "✅");
      toast("Kafka: order.placed + payment.success published", "kafka", "📨");
      loadSharding();
      sseSource.close();
    }

    // Capture kafka events for pipeline tab
    if(event === "pipeline" && data.status === "done") {
      // Load latest kafka into pipeline kafka panel
      loadPipelineKafka();
    }
  };
}

function resetPipeline() {
  for(let i=0;i<8;i++){
    const n=document.getElementById(`pnode-${i}`);
    if(n){n.classList.remove("active","done","fail");}
    const a=document.getElementById(`parr-${i}`);
    if(a)a.classList.remove("done");
  }
  document.getElementById("pipeline-log").innerHTML="";
  document.getElementById("pipeline-kafka").innerHTML=`<div style="color:#484f58;font-size:.73rem;padding:6px">Waiting for order to start…</div>`;
}

function animateNode(step, status) {
  for(let i=0;i<8;i++){
    const n=document.getElementById(`pnode-${i}`);
    if(!n)continue;
    if(i<step){n.classList.add("done");n.classList.remove("active","fail");}
    else if(i===step){
      n.classList.add(status==="done"?"done":status==="fail"?"fail":"active");
      n.classList.remove(status==="done"?"active":"done", status==="fail"?"active":"fail");
    } else {
      n.classList.remove("active","done","fail");
    }
  }
}

function renderPipelineLog() {
  const el = document.getElementById("pipeline-log");
  el.innerHTML = pipelineLogBuffer.join("");
  el.scrollTop = el.scrollHeight;
}

async function loadPipelineKafka() {
  const {events} = await api("/api/kafka");
  const COLORS = {"order-events":"#58a6ff","payment-events":"#e3b341","notifications":"#ff7b72","product-events":"#3fb950","analytics-events":"#d2a8ff","DLQ":"#f85149"};
  const recent = events.slice(0,30);
  document.getElementById("pipeline-kafka").innerHTML = recent.length
    ? recent.map(e=>`<div class="kf-row"><span class="kf-topic" style="color:${COLORS[e.topic]||'#8b949e'}">${e.topic}</span><span class="kf-payload">${e.payload}</span><span class="kf-ts">${e.ts}</span></div>`).join("")
    : `<div style="color:#484f58;font-size:.73rem;padding:6px">No events yet</div>`;
}

// ─── PIPELINE NODES INIT ────────────────────────────────────────────────────
(function initPipeline(){
  const steps=[
    {icon:"🔐",lbl:"1. Auth\nValidate"},
    {icon:"🛒",lbl:"2. Cart\nFetch"},
    {icon:"📦",lbl:"3. Stock\nCheck"},
    {icon:"📝",lbl:"4. Order\nCreate"},
    {icon:"💳",lbl:"5. Payment\nProcess"},
    {icon:"📉",lbl:"6. Inventory\nDeduct"},
    {icon:"✅",lbl:"7. Order\nConfirm"},
    {icon:"🗑️",lbl:"8. Cart\nClear"},
  ];
  const wrap = document.getElementById("pipeline-nodes");
  if(!wrap)return;
  let html="";
  steps.forEach((s,i)=>{
    html+=`<div class="p-node" id="pnode-${i}"><div class="p-node-icon">${s.icon}</div><div class="p-node-lbl">${s.lbl.replace("\n","<br>")}</div></div>`;
    if(i<7) html+=`<div class="p-arrow" id="parr-${i}"></div>`;
  });
  wrap.innerHTML=html;
})();

// ─── ORDERS ─────────────────────────────────────────────────────────────────
async function loadOrders() {
  document.getElementById("orders-sub").textContent = `Orders for ${USERS[currentUser].name}`;
  const orders = await api(`/api/orders?uid=${currentUser}`);
  const el = document.getElementById("orders-list");
  if(!orders.length){
    el.innerHTML=`<div class="empty"><div class="empty-icon">📦</div><p>No orders yet</p><button class="btn btn-p" style="margin-top:10px" onclick="nav('catalog')">Start Shopping</button></div>`;
    return;
  }
  el.innerHTML = orders.map(o=>`
    <div class="order-card">
      <div class="order-hdr">
        <span class="order-id">${o.order_id}</span>
        <span class="pill pill-${o.status.toLowerCase()}">${o.status}</span>
        <span class="chip chip-b">Shard ${o.shard}</span>
        <span style="margin-left:auto;font-size:.72rem;color:var(--ink-soft)">${o.timestamp}</span>
      </div>
      <table class="order-items-tbl">
        ${Object.values(o.items).map(item=>`
          <tr><td>${item.icon||"📦"} ${item.name} ×${item.quantity}</td><td style="text-align:right">₹${item.subtotal.toLocaleString('en-IN')}</td></tr>
        `).join("")}
      </table>
      <div style="display:flex;align-items:center;justify-content:space-between">
        <div class="order-total">Total: ₹${o.total.toLocaleString('en-IN')}</div>
        ${o.status==="CONFIRMED"?`<button class="btn btn-o btn-sm" onclick="cancelOrder('${o.order_id}')">Cancel Order</button>`:""}
      </div>
    </div>`).join("");
}

async function cancelOrder(oid) {
  const r = await api("/api/order/cancel", {body:{order_id:oid}});
  if(r.ok){ toast(`Order ${oid} cancelled. Refund in 3–5 days.`, "info", "↩️"); }
  else { toast("Cannot cancel this order", "error", "❌"); }
  loadOrders(); updateBadges();
}

// ─── KAFKA ──────────────────────────────────────────────────────────────────
async function loadKafka() {
  const {events, counts} = await api("/api/kafka");
  const COLORS = {"order-events":"#58a6ff","payment-events":"#e3b341","notifications":"#ff7b72","product-events":"#3fb950","analytics-events":"#d2a8ff","DLQ":"#f85149"};

  document.getElementById("kafka-counts").innerHTML = Object.entries(counts).map(([t,c])=>`
    <div class="metric"><div class="metric-val">${c}</div><div class="metric-lbl">${t}</div></div>`).join("");

  document.getElementById("kafka-feed").innerHTML = events.length
    ? events.map(e=>`<div class="kf-row"><span class="kf-topic" style="color:${COLORS[e.topic]||'#8b949e'}">${e.topic}</span><span class="kf-payload">${e.payload}</span><span class="kf-ts">${e.ts}</span></div>`).join("")
    : `<div style="color:#484f58;font-size:.75rem;padding:8px">No events yet. Place an order to generate Kafka events.</div>`;

  const topics = [
    {name:"order-events",parts:6,ret:"7 days",key:"userId",cg:"notification, inventory, dispatch, analytics"},
    {name:"payment-events",parts:3,ret:"30 days",key:"orderId",cg:"order-svc, notification, payment-verify"},
    {name:"notifications",parts:3,ret:"1 day",key:"userId",cg:"push-svc, sms-svc, email-svc"},
    {name:"product-events",parts:6,ret:"7 days",key:"productId",cg:"search-sync, analytics"},
    {name:"analytics-events",parts:6,ret:"90 days",key:"orderId",cg:"analytics → BigQuery"},
    {name:"DLQ",parts:3,ret:"14 days",key:"original-topic",cg:"on-call engineer (manual replay)"},
  ];
  document.getElementById("kafka-topics").innerHTML = topics.map(t=>`
    <div class="card">
      <div style="font-weight:700;color:var(--primary);font-size:.83rem;font-family:var(--mono);margin-bottom:8px">${t.name}</div>
      <div style="font-size:.73rem;color:var(--ink-soft);line-height:1.8">
        <div>🔢 <b>${t.parts}</b> partitions · ⏱ ${t.ret}</div>
        <div>🔑 Key: <code>${t.key}</code></div>
        <div style="margin-top:5px;padding-top:5px;border-top:1px solid var(--border)">👥 ${t.cg}</div>
      </div>
    </div>`).join("");
}

// ─── SHARDING ───────────────────────────────────────────────────────────────
async function loadSharding() {
  const {real, demo, detail, total} = await api("/api/sharding");
  const colors=["#0F3460","#E63946","#2D6A4F"];

  function bars(counts, tot, elId) {
    const el = document.getElementById(elId);
    if(!el)return;
    el.innerHTML = counts.map((c,i)=>{
      const pct = tot>0?Math.round(c/tot*100):0;
      return `<div class="sh-bar-wrap">
        <div class="sh-bar-lbl"><span>Shard ${i} (AZ-${["A","B","C"][i]})</span><span>${c} orders · ${pct}%</span></div>
        <div class="sh-bar-track"><div class="sh-bar-fill" style="width:${pct}%;background:${colors[i]}">${pct>10?pct+"%":""}</div></div>
      </div>`;
    }).join("");
  }

  bars(real, total, "real-shards");
  bars(demo, 30, "demo-shards");
}

// ─── ARCHITECTURE ────────────────────────────────────────────────────────────
const ARCH = [
  {icon:"🌐",bg:"#dde9f7",title:"Client Layer",sub:"Mobile App · Web App · Seller Dashboard · Admin Panel",
   desc:"All user-facing interfaces communicate over HTTPS + TLS 1.3. React Native for mobile, Next.js SSR for web.",services:[]},
  {icon:"🚦",bg:"#fff3cd",title:"API Gateway + CDN",sub:"CDN · Load Balancer · Auth Middleware · Rate Limiter · Circuit Breaker",
   desc:"Single entry point. Cloudflare CDN at edge. JWT validation (15-min tokens). Redis Token Bucket (200 req/min). Circuit Breaker fails fast.",
   services:[
     {name:"CDN",tech:"Cloudflare",desc:"Static assets, DDoS protection"},
     {name:"Load Balancer",tech:"Nginx / AWS ALB",desc:"Round-robin + health checks"},
     {name:"Auth Middleware",tech:"JWT + Redis",desc:"15-min access, 30-day refresh"},
     {name:"Rate Limiter",tech:"Redis Token Bucket",desc:"200 req/min per user"},
     {name:"Circuit Breaker",tech:"Hystrix",desc:"Fail-fast on downstream issues"},
     {name:"WAF",tech:"AWS Shield",desc:"SQL injection, DDoS protection"},
   ]},
  {icon:"⚙️",bg:"#d8f3dc",title:"Microservices (10 services)",sub:"Docker + Kubernetes · HPA 5→200 pods · gRPC (sync) + Kafka (async)",
   desc:"Each service owns its data, scales independently. K8s HPA auto-scales from 5 to 200 pods during flash sales.",
   services:[
     {name:"User / Auth",tech:"Node.js + PostgreSQL",desc:"JWT, bcrypt, OAuth 2.0, account lock"},
     {name:"Product",tech:"Node.js + MongoDB",desc:"Catalog CRUD, stock reservation"},
     {name:"Search",tech:"Node.js + Elasticsearch",desc:"BM25, fuzzy, geo, 5-min Redis cache"},
     {name:"Cart",tech:"Node.js + Redis",desc:"Ephemeral TTL store, price calc"},
     {name:"Order",tech:"Node.js/Go + PostgreSQL",desc:"8-step orchestration, state machine"},
     {name:"Payment",tech:"Java/Spring + PostgreSQL",desc:"Idempotency, Razorpay, retry 3×"},
     {name:"Notification",tech:"Node.js + FCM/APNS",desc:"Push, SMS, Email fan-out, DLQ"},
     {name:"Delivery",tech:"Go + Redis GEO",desc:"Agent geo-match, GPS 5s, ETA"},
     {name:"Analytics",tech:"Apache Flink + BigQuery",desc:"Real-time GMV, dashboards"},
     {name:"Review",tech:"Node.js + MongoDB",desc:"Verified-buyer, moderation queue"},
   ]},
  {icon:"📨",bg:"#ede7f6",title:"Kafka Message Bus",sub:"3 Brokers · 3 AZs · RF=3 · acks=all · idempotent=true",
   desc:"Decouples all services. One order.placed event fans out to notification, inventory, dispatch, analytics independently. DLQ for failed retries.",services:[]},
  {icon:"🗄️",bg:"#fce8e8",title:"Data Layer (Polyglot Persistence)",sub:"PostgreSQL · MongoDB · Redis · Elasticsearch · InfluxDB · S3 · BigQuery",
   desc:"Right database for the right job: PostgreSQL (ACID for payments), MongoDB (flexible catalog), Redis (sub-ms cache), Elasticsearch (full-text search).",
   services:[
     {name:"PostgreSQL 15",tech:"ACID RDBMS",desc:"Orders, payments, users"},
     {name:"MongoDB 7",tech:"Document Store",desc:"Products, reviews, catalog"},
     {name:"Redis 7.2",tech:"Cache + Sessions",desc:"Cart TTL, search cache, JWT"},
     {name:"Elasticsearch 8",tech:"Search Index",desc:"BM25, fuzzy, geo, ranking"},
     {name:"InfluxDB",tech:"Time-Series",desc:"GPS tracks, price history"},
     {name:"BigQuery",tech:"Data Warehouse",desc:"Analytics SQL, ML data"},
   ]},
  {icon:"🔌",bg:"#e8f5e9",title:"External Services",sub:"Razorpay · Google Maps · FCM · Twilio · SendGrid · Prometheus",
   desc:"Third-party integrations wrapped in Circuit Breaker + retry logic. All payment calls use idempotency keys.",
   services:[
     {name:"Razorpay / Stripe",tech:"Payment Gateway",desc:"Card, UPI, wallet; webhooks"},
     {name:"Google Maps API",tech:"ETA + Geocoding",desc:"Distance Matrix, directions"},
     {name:"FCM / APNS",tech:"Push Notifications",desc:"Android + iOS, <1s delivery"},
     {name:"Twilio",tech:"SMS Gateway",desc:"OTP, order alerts"},
     {name:"SendGrid",tech:"Email Service",desc:"Receipts, promotions"},
     {name:"Prometheus + Grafana",tech:"Monitoring",desc:"Metrics, alerts, PagerDuty"},
   ]},
];

function renderArch() {
  const el = document.getElementById("arch-layers");
  el.innerHTML = ARCH.map((l,i)=>`
    <div class="arch-layer" id="archlayer-${i}" onclick="toggleArch(${i})">
      <div class="arch-hdr">
        <div class="arch-icon" style="background:${l.bg}">${l.icon}</div>
        <div>
          <div style="font-weight:700;font-size:.9rem;color:var(--ink)">${l.title}</div>
          <div style="font-size:.73rem;color:var(--ink-soft);margin-top:1px">${l.sub}</div>
        </div>
        <div class="arch-toggle">▶</div>
      </div>
      <div class="arch-body">
        <p style="font-size:.8rem;color:var(--ink-mid);line-height:1.65;margin-bottom:10px">${l.desc}</p>
        ${l.services.length?`<div class="svc-grid">${l.services.map(s=>`
          <div class="svc-card">
            <div class="svc-name">${s.name}</div>
            <div class="svc-tech">${s.tech}</div>
            <div class="svc-desc">${s.desc}</div>
          </div>`).join("")}</div>`:""}
      </div>
    </div>`).join("");
}

function toggleArch(i) {
  document.getElementById(`archlayer-${i}`).classList.toggle("open");
}

// ─── LOGS ───────────────────────────────────────────────────────────────────
async function loadLogs() {
  const kind = document.getElementById("log-filter").value;
  const logs = await api(`/api/logs?kind=${kind}`);
  const el = document.getElementById("log-console");
  if(!logs.length){el.innerHTML=`<span style="color:#484f58">No logs yet.</span>`;return;}
  el.innerHTML = logs.map(l=>`
    <div class="log-row">
      <span class="log-ts">[${l.ts}]</span>
      <span class="log-${l.kind}">${l.msg.replace(/&/g,"&amp;").replace(/</g,"&lt;")}</span>
    </div>`).join("");
}

// ─── ABOUT ──────────────────────────────────────────────────────────────────
function renderAbout() {
  const Qs=[
    ["Q1","Requirements Analysis","Functional & non-functional requirements with justifications"],
    ["Q2","System Architecture","Microservices interaction, component breakdown"],
    ["Q3","Order Processing Flow","Browse → Cart → Checkout → Payment → Delivery"],
    ["Q4","Database Design","SQL + NoSQL polyglot persistence with schema examples"],
    ["Q5","Python Implementation","shopease_app.py — Flask microservice simulation"],
    ["Q6","Scalability & Fault Tolerance","5-stage scaling, circuit breakers, DLQ, shard replication"],
  ];
  document.getElementById("ab-questions").innerHTML = Qs.map(([q,t,d])=>`
    <div style="display:flex;gap:9px;padding:7px 0;border-bottom:1px solid var(--border)">
      <span class="chip chip-b">${q}</span>
      <div><div style="font-weight:600;font-size:.8rem">${t}</div><div style="font-size:.72rem;color:var(--ink-soft)">${d}</div></div>
    </div>`).join("");

  const svcs=["Auth Service (bcrypt + JWT)","Product Service (catalog + stock)","Search Service (keyword filter)","Cart Service (Redis TTL sim)","Order Service (8-step orchestration)","Payment Service (idempotency + retry sim)","Notification Service (simulated)","DB Sharding (MD5 hash)"];
  document.getElementById("ab-services").innerHTML = svcs.map(s=>
    `<div style="padding:5px 0;border-bottom:1px solid var(--border);font-size:.8rem;display:flex;gap:7px"><span style="color:var(--green)">✓</span>${s}</div>`).join("");

  const stack=[
    ["Backend","Python + Flask (this demo)"],["Frontend","Vanilla JS + CSS (no framework)"],
    ["Message Bus","Apache Kafka (3 brokers, RF=3)"],["Cache","Redis 7.2 (TTL, Pub/Sub)"],
    ["Relational DB","PostgreSQL 15 + PgBouncer"],["Document DB","MongoDB 7.0 (sharded)"],
    ["Search","Elasticsearch 8 (BM25 + fuzzy)"],["Containers","Docker + Kubernetes (HPA)"],
    ["Payment","Razorpay / Stripe (idempotency)"],["Monitoring","Prometheus + Grafana"],
  ];
  document.getElementById("ab-stack").innerHTML = stack.map(([k,v])=>
    `<div style="display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid var(--border);font-size:.75rem">
      <span style="font-weight:600;color:var(--ink-mid)">${k}</span>
      <span style="color:var(--ink-soft)">${v}</span>
    </div>`).join("");
}

// ─── TOAST ──────────────────────────────────────────────────────────────────
function toast(msg, type="info", icon="ℹ️") {
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${icon}</span><span class="toast-msg">${msg}</span>`;
  document.getElementById("toasts").appendChild(el);
  setTimeout(()=>{el.style.opacity="0";el.style.transition="opacity .3s";setTimeout(()=>el.remove(),300);}, 3000);
}

// ─── RESET ──────────────────────────────────────────────────────────────────
async function resetAll() {
  if(!confirm("Reset all data? Orders, cart, payments and logs will be cleared.")) return;
  await api("/api/reset", {body:{}});
  toast("Platform reset ↺", "info", "↺");
  updateBadges();
  loadCatalog();
}

// ─── BOOT ───────────────────────────────────────────────────────────────────
(async function boot(){
  renderArch();
  renderAbout();
  await loadCatalog();
  await updateBadges();
  await loadSharding();
  // Auto-refresh stats every 5s
  setInterval(loadTopStats, 5000);
})();
</script>
</body>
</html>"""

# ─── ENTRYPOINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    # Seed some startup logs
    add_log("── ShopEase Platform Initialised ──", "system")
    add_log("Auth Service: Ready (bcrypt + JWT)", "auth")
    add_log("Product Service: Catalog loaded — 6 products", "product")
    add_log("Cart Service: Redis TTL store ready (30-min TTL per user)", "cart")
    add_log("Payment Service: Idempotency engine online", "payment")
    add_log("Order Service: 8-step state machine initialised", "order")
    add_log("Kafka: 3 brokers online — AZ-A, AZ-B, AZ-C (RF=3, acks=all)", "kafka")
    add_log("Notification Service: FCM + APNS + Twilio + SendGrid ready", "notif")
    add_log("Search Service: Elasticsearch index loaded (BM25 ranking)", "product")
    add_log("Analytics Service: Apache Flink stream processor ready", "system")

    print("\n" + "="*60)
    print("  ShopEase E-Commerce Platform")
    print("="*60)
    print("\n  Pages:")
    print("    🛍️  Catalog    — Browse & add to cart")
    print("    🛒  Cart       — Review & place order")
    print("    ⚡  Live Flow  — Watch 8-step pipeline animate")
    print("    📨  Kafka      — Real-time event stream")
    print("    🔀  Sharding   — DB hash distribution")
    print("    🗺️  Architecture — Full system design")
    print("    📊  Diagrams   — Architecture diagrams")
    print("    🖥️  Logs       — Microservice activity")
    print("\n  Press Ctrl+C to stop\n" + "="*60 + "\n")
    
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
