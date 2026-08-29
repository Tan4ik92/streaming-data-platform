import uuid
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size
        self._logger = logger

    def _uuid(self, value: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_OID, value)

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START CDM")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            if "payload" not in msg:
                continue

            payload = msg["payload"]

            user_id = self._uuid(payload["user"]["id"])

            for product in payload["products"]:
                product_id = self._uuid(product["id"])
                category_id = self._uuid(product["category"])

                self._cdm_repository.user_product_counter_upsert(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product["name"]
                )

                self._cdm_repository.user_category_counter_upsert(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=product["category"]
                )

            self._logger.info("CDM counters updated")

        self._logger.info(f"{datetime.utcnow()}: FINISH CDM")