# Streaming Data Platform

An event-driven data platform that processes restaurant order events through independent STG, DDS and CDM services.

## Architecture

```text
Kafka: order events
        ↓
STG service ──→ PostgreSQL STG ──→ Kafka
                                      ↓
                                 DDS service
                                      ↓
                            PostgreSQL Data Vault
                                      ↓
                                    Kafka
                                      ↓
                                 CDM service
                                      ↓
                         PostgreSQL analytics marts
```

Redis is used by the STG service to enrich events with user, restaurant and product reference data.

## Data model

The DDS service implements Data Vault entities:

- Hubs: users, orders, products, categories and restaurants.
- Links: order-user, order-product, product-category and product-restaurant.
- Satellites: names, order cost and order status.

The CDM service maintains user-product and user-category counters with idempotent PostgreSQL UPSERT operations.

## Technologies

Python, Kafka, PostgreSQL, Redis, Docker, Kubernetes, Helm, Yandex Cloud.

## Repository structure

```text
services/
  stg/
  dds/
  cdm/
deploy/helm/
```

## Configuration and security

Runtime configuration is passed to Kubernetes through ConfigMaps for non-sensitive settings and Secrets for credentials. The included `secret.example.yaml` contains placeholders only.

Create a local secret manifest from the example and never commit real credentials. No credentials or private datasets are stored in this repository.
