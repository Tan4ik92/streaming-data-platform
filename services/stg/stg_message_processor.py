import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis_client: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._batch_size = batch_size
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if msg is None:
                break

            if "payload" not in msg:
                self._logger.info("Skip technical message without payload")
                continue

            object_id = msg["object_id"]
            object_type = msg["object_type"]
            sent_dttm = datetime.strptime(msg["sent_dttm"], "%Y-%m-%d %H:%M:%S")
            payload = msg["payload"]

            self._stg_repository.order_events_insert(
                object_id=object_id,
                object_type=object_type,
                sent_dttm=sent_dttm,
                payload=json.dumps(payload, ensure_ascii=False)
            )

            user_id = payload["user"]["id"]
            user = self._redis.get(user_id)

            restaurant_id = payload["restaurant"]["id"]
            restaurant = self._redis.get(restaurant_id)

            products = []
            menu = restaurant["menu"]

            for item in payload["order_items"]:
                product_id = item["id"]

                menu_item = next(
                    x for x in menu
                    if x["_id"] == product_id
                )

                products.append({
                    "id": product_id,
                    "price": item["price"],
                    "quantity": item["quantity"],
                    "name": menu_item["name"],
                    "category": menu_item["category"]
                })

            output_msg = {
                "object_id": object_id,
                "object_type": object_type,
                "payload": {
                    "id": object_id,
                    "date": payload["date"],
                    "cost": payload["cost"],
                    "payment": payload["payment"],
                    "status": payload["final_status"],
                    "restaurant": {
                        "id": restaurant_id,
                        "name": restaurant["name"]
                    },
                    "user": {
                        "id": user_id,
                        "name": user["name"]
                    },
                    "products": products
                }
            }

            self._producer.produce(output_msg)
            self._logger.info("Message Sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")