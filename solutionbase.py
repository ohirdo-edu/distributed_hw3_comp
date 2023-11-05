from random import Random
from typing import NamedTuple
import datetime


SHOP_NAMES = [f"Shop_{i}" for i in range(10)]
PRODUCT_NAMES = [f"Product_{i}" for i in range(10)]
RECEIPT_COUNT = 1000


class Entry(NamedTuple):
    receipt_id: int
    shop: str
    product: str
    total_price: int
    quantity: float
    timestamp: datetime.datetime


class DateInterval(NamedTuple):
    start: datetime.datetime
    end: datetime.datetime


class Solution:
    def populate(self, entries: [Entry]):
        pass

    def get_total_amount(self) -> float:
        pass

    def get_total_price(self) -> float:
        pass

    def get_total_price_for_date_interval(self, date_interval: DateInterval) -> float:
        pass

    def get_amount_at_shop(self, product: str, shop: str, date_interval: DateInterval) -> float:
        pass

    def get_amount(self, product: str, date_interval: DateInterval) -> float:
        pass

    def get_total_price_by_shop(self, date_interval: DateInterval):
        pass

    def get_top_products_by_2(self, date_interval: DateInterval, limit: int):
        pass

    def get_top_products_by_3(self, date_interval: DateInterval, limit: int):
        pass

    def get_top_products_by_4(self, date_interval: DateInterval, limit: int):
        pass


def generate_entries(random: Random = Random(42)) -> [Entry]:
    entries: list[Entry] = []
    start_timestamp = 1537381286
    end_timestamp = 1697381286
    for receipt_id in range(RECEIPT_COUNT):
        shop_name = random.choice(SHOP_NAMES)
        product_names = list(PRODUCT_NAMES)
        random.shuffle(product_names)
        timestamp = random.randrange(start_timestamp, end_timestamp)

        for product_name in product_names[:random.randint(1, len(PRODUCT_NAMES))]:
            entries.append(Entry(
                receipt_id=receipt_id,
                shop=shop_name,
                product=product_name,
                total_price=random.randint(10, 2000),
                quantity=3 * random.random(),
                timestamp=datetime.datetime.utcfromtimestamp(timestamp),
            ))

    return entries


if __name__ == '__main__':
    print(generate_entries())
