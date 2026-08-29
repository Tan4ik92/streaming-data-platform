import json
import uuid
from datetime import datetime
from decimal import Decimal
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    LOAD_SRC = "stg-service-kafka"

    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        dds_repository: DdsRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger

    def _uuid(self, value: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_OID, value)

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START DDS")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            if "payload" not in msg:
                continue

            payload = msg["payload"]
            load_dt = datetime.utcnow()

            order_id = int(payload["id"])
            order_dt = datetime.strptime(payload["date"], "%Y-%m-%d %H:%M:%S")

            user = payload["user"]
            restaurant = payload["restaurant"]
            products = payload["products"]

            h_order_pk = self._uuid(str(order_id))
            h_user_pk = self._uuid(user["id"])
            h_restaurant_pk = self._uuid(restaurant["id"])

            self._dds_repository.h_order_insert(
                h_order_pk,
                order_id,
                order_dt,
                load_dt,
                self.LOAD_SRC
            )

            self._dds_repository.h_user_insert(
                h_user_pk,
                user["id"],
                load_dt,
                self.LOAD_SRC
            )

            self._dds_repository.h_restaurant_insert(
                h_restaurant_pk,
                restaurant["id"],
                load_dt,
                self.LOAD_SRC
            )

            self._dds_repository.s_user_names_insert(
                h_user_pk,
                user["name"],
                user.get("login", ""),
                load_dt,
                self.LOAD_SRC,
                self._uuid(f'{user["id"]}|{user["name"]}|{user.get("login", "")}')
            )

            self._dds_repository.s_restaurant_names_insert(
                h_restaurant_pk,
                restaurant["name"],
                load_dt,
                self.LOAD_SRC,
                self._uuid(f'{restaurant["id"]}|{restaurant["name"]}')
            )

            self._dds_repository.l_order_user_insert(
                self._uuid(f'{order_id}|{user["id"]}'),
                h_order_pk,
                h_user_pk,
                load_dt,
                self.LOAD_SRC
            )

            self._dds_repository.s_order_cost_insert(
                h_order_pk,
                Decimal(str(payload["cost"])),
                Decimal(str(payload["payment"])),
                load_dt,
                self.LOAD_SRC,
                self._uuid(f'{order_id}|{payload["cost"]}|{payload["payment"]}')
            )

            self._dds_repository.s_order_status_insert(
                h_order_pk,
                payload["status"],
                load_dt,
                self.LOAD_SRC,
                self._uuid(f'{order_id}|{payload["status"]}')
            )

            for product in products:
                h_product_pk = self._uuid(product["id"])
                h_category_pk = self._uuid(product["category"])

                self._dds_repository.h_product_insert(
                    h_product_pk,
                    product["id"],
                    load_dt,
                    self.LOAD_SRC
                )

                self._dds_repository.h_category_insert(
                    h_category_pk,
                    product["category"],
                    load_dt,
                    self.LOAD_SRC
                )

                self._dds_repository.s_product_names_insert(
                    h_product_pk,
                    product["name"],
                    load_dt,
                    self.LOAD_SRC,
                    self._uuid(f'{product["id"]}|{product["name"]}')
                )

                self._dds_repository.l_order_product_insert(
                    self._uuid(f'{order_id}|{product["id"]}'),
                    h_order_pk,
                    h_product_pk,
                    load_dt,
                    self.LOAD_SRC
                )

                self._dds_repository.l_product_restaurant_insert(
                    self._uuid(f'{product["id"]}|{restaurant["id"]}'),
                    h_product_pk,
                    h_restaurant_pk,
                    load_dt,
                    self.LOAD_SRC
                )

                self._dds_repository.l_product_category_insert(
                    self._uuid(f'{product["id"]}|{product["category"]}'),
                    h_product_pk,
                    h_category_pk,
                    load_dt,
                    self.LOAD_SRC
                )

            self._producer.produce(msg)
            self._logger.info("DDS message processed and sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH DDS")