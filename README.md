# ShopEase — Scalable E-Commerce Platform
### B.Tech CSE 2024-28 · System Design Assignment · Semester IV · ITM Skills University

---

## Table of Contents
1. [Problem Statement](#problem-statement)
2. [Requirements Analysis](#requirements-analysis)
3. [System Architecture](#system-architecture)
4. [Component Breakdown](#component-breakdown)
5. [Database Design](#database-design)
6. [Kafka Event Architecture](#kafka-event-architecture)
7. [Order Flow Walkthrough](#order-flow-walkthrough)
8. [Scalability & Fault Tolerance](#scalability--fault-tolerance)
9. [Python Implementation](#python-implementation)
10. [Key Design Decisions](#key-design-decisions)
11. [How to Run](#how-to-run)

---

## Problem Statement

ShopEase is a large-scale e-commerce platform similar to Amazon. Millions of users browse products, add items to cart, and place orders. The system faces challenges during sales events — slow page loading, order processing delays, and difficulty scaling under peak demand.

The goal is to design a **scalable, reliable, high-performance** e-commerce backend using microservices, distributed databases, caching, and Kafka-based async messaging.

---

## Requirements Analysis

### Functional Requirements

| Requirement | Why It Matters |
|---|---|
| Product Management | Users must browse a large, categorized catalog efficiently |
| Search Engine | Fast, relevant product discovery drives purchases |
| Cart Management | Users add, update, and clear items before checkout |
| Order Processing | Orders must be created, tracked, and fulfilled reliably |
| Payment Integration | Transactions must be secure and idempotent (no double charges) |
| User Auth (JWT + OAuth) | Secure login prevents unauthorized access |
| Notifications | Users need real-time updates on order status |

### Non-Functional Requirements

| Requirement | Target | Mechanism |
|---|---|---|
| High Scalability | Millions of concurrent users | Kubernetes HPA, Kafka buffering, horizontal DB sharding |
| High Availability | 99.9% uptime | Multi-region, replica failover < 30s |
| Fault Tolerance | Single service failure isolated | Circuit Breaker (Hystrix), DLQ, retry with backoff |
| Low Latency | Search < 200ms, checkout < 2s | Redis cache, Elasticsearch, CDN edge |
| Data Consistency | Payment ACID guarantees | PostgreSQL + idempotency keys |

---

## System Architecture

```
CLIENT LAYER
  Mobile App (React Native)  |  Web App (Next.js)  |  Merchant Dashboard  |  Admin Panel
          |
          ↓  HTTPS + TLS 1.3
API GATEWAY LAYER
  CDN (Cloudflare) → Load Balancer → Auth Middleware → Rate Limiter → API Router
          |
          ↓  gRPC (sync, internal)
MICROSERVICES LAYER  (Docker + Kubernetes)
  User Service | Product Service | Search Service | Cart Service
  Order Service | Payment Service | Notification Service | Analytics Service
          |
          ↓  Kafka (async events)
KAFKA MESSAGE BUS  (3 Brokers, Replication=3, acks=all)
  order-events | payment-events | product-events | notifications | DLQ
          |
          ↓
DATA LAYER  (Polyglot Persistence)
  PostgreSQL | MongoDB | Redis | Elasticsearch | S3 | BigQuery
          |
          ↓
EXTERNAL SERVICES
  Stripe/Razorpay | Google Maps | FCM/APNS | Twilio | SendGrid | Prometheus+Grafana
```

The architecture file `shopease_architecture.xml` can be opened in [draw.io](https://app.diagrams.net) for the full visual diagram.

---

## Component Breakdown

### API Gateway
The single entry point for all client traffic. Handles:
- JWT validation (15-minute access tokens, 30-day refresh tokens)
- Rate limiting via Redis Token Bucket (100 req/min per user)
- Routing to the correct microservice via gRPC
- Circuit breaking — if a downstream service is unhealthy, requests fail fast instead of hanging

### Microservices

**User / Auth Service** — Registration, login with bcrypt password hashing, JWT issuance, OAuth 2.0 (Google/Facebook), account locking after 3 failed attempts.

**Product Service** — CRUD for product catalog, stock reservation, inventory management. Uses MongoDB for flexible schema (products have varying attributes across categories).

**Search Service** — Elasticsearch-backed full-text and geo search. Debounce 300ms on frontend. Autocomplete via Redis Sorted Sets. Results cached in Redis with 5-minute TTL.

**Cart Service** — Ephemeral cart stored in Redis with TTL. Validates stock availability before adding items. Calculates subtotals, GST, and delivery fees.

**Order Service** — Orchestrates the entire checkout flow. Creates the order record, coordinates payment, triggers stock deduction, and publishes events to Kafka. Implements the order state machine.

**Payment Service** — Idempotency key (hash of userId + cartId + amount) prevents double charges. Calls Stripe/Razorpay with exponential backoff retry (1s → 2s → 4s). Publishes `payment.success` or `payment.failed` events.

**Notification Service** — Consumes Kafka notification events and fans out to FCM (Android push), APNS (iOS push), Twilio SMS, and SendGrid email. Retries 3× per channel; failures go to DLQ.

**Analytics Service** — Consumes all Kafka topics via Apache Flink. Streams computed metrics (GMV, orders/min) to BigQuery/Redshift for dashboards.

---

## Database Design

ShopEase uses **polyglot persistence** — different databases for different access patterns.

### PostgreSQL (Relational — ACID)
Used for data where correctness is critical: orders, users, payments.

```
users         (user_id PK, name, email, password_hash, created_at)
products      (product_id PK, name, price, stock, category_id)
orders        (order_id PK, user_id FK, total, status, created_at)
order_items   (item_id PK, order_id FK, product_id FK, quantity, price)
payments      (payment_id PK, order_id FK, amount, status, idempotency_key)
```

Partitioned by `created_at` date for query performance. One primary + two read replicas. PgBouncer for connection pooling (200 connections).

### MongoDB (Document — Flexible Schema)
Used for the product catalog and reviews, where attributes vary per category.

```javascript
// products collection
{ "_id": "p001", "name": "Wireless Headphones", "price": 1999,
  "specs": { "battery": "20hr", "driver": "40mm" },
  "images": ["img1.jpg", "img2.jpg"] }

// reviews collection
{ "product_id": "p001", "user_id": "u001", "rating": 4,
  "title": "Great sound", "body": "...", "created_at": "..." }
```

Sharded on `product_id` for horizontal scaling. TTL index on temporary/draft data.

### Redis (Cache + Session)
- Cart data: `cart:{user_id}` with 30-minute TTL
- Search cache: `search:{hash(query+lat+lng)}` with 5-minute TTL
- JWT blacklist (on logout)
- Rate limit counters: `ratelimit:{user_id}` with 1-minute TTL
- Failed login counters: `failed:{email}` with 15-minute TTL

### Elasticsearch
Index: `products`, `categories`. BM25 full-text scoring with custom ranking:
```
Score = 0.40 × relevance + 0.30 × proximity + 0.20 × rating + 0.10 × availability
```
Synced from MongoDB via Kafka Connect Debezium CDC.

### Why Not One Database?
Using only PostgreSQL would require complex full-text indexing for search and rigid schemas for catalog data. Using only MongoDB would lose ACID guarantees for payments. The polyglot approach matches the database type to the problem — the same principle used in real systems like Amazon and Flipkart.

---

## Kafka Event Architecture

Kafka decouples services. When an order is placed, the Order Service publishes one event — and multiple consumers react independently.

```
Order Service publishes → order-events/order.placed
     ↓ consumed by notification-group  → push notification to user
     ↓ consumed by analytics-group     → update GMV dashboard
     ↓ consumed by inventory-group     → reserve stock

Payment Service publishes → payment-events/payment.success
     ↓ consumed by order service       → confirm order status
     ↓ consumed by notification-group  → "Payment successful" SMS
```

**Key Kafka settings:**
- `acks=all` — producer waits for all in-sync replicas before ACK
- `enable.idempotence=true` — no duplicate messages on retry
- `replication.factor=3` — survives 2 broker failures
- `min.insync.replicas=2` — at least 2 replicas must acknowledge
- Dead Letter Queue (DLQ) — messages that fail 3 consumer retries are parked for manual replay

---

## Order Flow Walkthrough

```
1. User taps "Place Order"
   → POST /orders (JWT validated at gateway)

2. Order Service validates:
   - Restaurant/merchant open?
   - All items in stock?
   - Address geocodable?

3. Order created in PostgreSQL → status: PENDING_PAYMENT

4. Payment Service:
   - Check idempotency key in Redis (prevent double charge)
   - Call Stripe/Razorpay
   - Retry 3× with exponential backoff if failure

5. On payment success:
   - Order status → PLACED → CONFIRMED
   - Stock decremented in Product Service
   - Kafka publishes: order.placed + payment.success

6. Notification Service consumes events:
   - Push notification (FCM/APNS)
   - SMS (Twilio)
   - Email receipt (SendGrid)

7. Cart cleared from Redis
```

---

## Scalability & Fault Tolerance

### Scaling to Millions of Users

| Stage | Users | Approach |
|---|---|---|
| Stage 1 | < 1,000 | Single server, monolith |
| Stage 2 | < 1M | Add read replicas + Redis cache |
| Stage 3 | < 10M | CDN + vertical scale + microservices |
| Stage 4 | < 100M | Horizontal sharding + Kafka + K8s HPA |
| Stage 5 | 1B+ | Multi-region geo sharding + global load balancing |

### Handling Traffic Spikes (Flash Sales)
Kafka acts as a buffer — it absorbs thousands of simultaneous order requests without dropping any. K8s Horizontal Pod Autoscaler scales Order Service pods from 5 to 200 based on CPU. PgBouncer manages DB connection pooling so the database is not overwhelmed.

### Fault Tolerance Patterns

**Circuit Breaker** — If the Payment Gateway becomes slow (> 2s), the circuit opens. Subsequent requests fail immediately with a friendly error instead of waiting and causing cascading failures across the system.

**Retry with Exponential Backoff** — Payment calls retry at 1s, 2s, 4s intervals. This handles transient network issues without hammering a struggling service.

**Shard Replication** — Each PostgreSQL shard has a primary + replica. If the primary fails, the replica is promoted in under 30 seconds. Zero data loss using synchronous replication.

**DLQ (Dead Letter Queue)** — Kafka messages that fail 3 consumer retries go to a DLQ topic instead of being dropped. On-call engineers inspect and replay them manually. No silent data loss.

**Idempotency** — The payment idempotency key (`hash(userId + cartId + amount)`) ensures that if the client retries a request (e.g., after a timeout), the payment is not charged twice.

---

## Python Implementation

`shopease_app.py` simulates the core e-commerce flows in pure Python.

### What It Implements

| Module | Functions |
|---|---|
| Auth Service | `auth_service_validate_user()` |
| Product Service | `product_service_get_product()`, `product_service_search()`, `product_service_check_stock()`, `product_service_reduce_stock()` |
| Cart Service | `cart_service_add_item()`, `cart_service_remove_item()`, `cart_service_view_cart()`, `cart_service_clear()` |
| Payment Service | `payment_service_process()` with idempotency check |
| Order Service | `order_service_place_order()` — full orchestration, `order_service_get_order()`, `order_service_cancel_order()` |
| Sharding Demo | `get_shard()`, `demo_sharding()` — hash-based distribution |

### Demo Flows Covered

1. Product search by keyword
2. Cart add/remove with stock validation
3. Full order placement (auth → cart → payment → stock deduction → confirmation)
4. Empty cart order rejection
5. Order cancellation with refund simulation
6. Hash-based shard distribution of 30 orders across 3 shards

---

## Key Design Decisions

**Why Kafka instead of direct service calls?**
Direct calls create tight coupling — if the Notification Service is down, the Order Service would fail too. Kafka lets each service work independently. The order is confirmed the moment it's written to Kafka; notifications may lag by milliseconds but orders never fail because of a notification outage.

**Why Redis for Cart instead of PostgreSQL?**
Carts are temporary, read/written frequently, and can be lost if a user abandons checkout. Redis provides sub-millisecond access and automatic TTL expiry. PostgreSQL's row-level locking would be overkill.

**Why Elasticsearch for Search instead of PostgreSQL LIKE queries?**
`LIKE '%pizza%'` on a 10-million-product catalog would perform a full table scan. Elasticsearch maintains an inverted index that returns results in milliseconds, supports fuzzy matching (`piiza` → `pizza`), geo-distance filtering, and custom ranking scores.

**Why hash sharding on order_id instead of user_id?**
 Sharding by user_id creates hotspots when one user places many orders (e.g., a business account). Sharding by order_id distributes load evenly. Cross-shard queries (e.g., "all orders by user X") scan all shards, but this is an acceptable trade-off since it is an infrequent admin query.

---

## How to Run

```bash
# Run the Python simulation
python shopease_app.py
```

**To open the architecture diagram:**
1. Go to [https://app.diagrams.net](https://app.diagrams.net)
2. File → Open → select `shopease_architecture.xml`
3. The full black-and-white architecture renders with all layers and connections

### Dependencies
- Python 3.8+ (no external packages required — uses only `datetime` and `hashlib` from standard library)
- draw.io (free, browser-based) to view the XML architecture

---

