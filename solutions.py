from collections import defaultdict
from itertools import product

from solutionbase import Solution, Entry, DateInterval


class RelationalSolution(Solution):

    def __init__(self, connection):
        self.connection = connection

    def populate(self, entries: [Entry]):
        with self.connection:
            with self.connection.cursor() as cursor:
                with open("relational.sql", "r") as f:
                    cursor.execute(f.read())

                for entry in entries:
                    cursor.execute(
                        """INSERT INTO entries (receipt_id, shop, product, total_price, quantity, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s)""",
                        (
                            entry.receipt_id, entry.shop, entry.product, entry.total_price,
                            entry.quantity, entry.timestamp
                        )
                    )

    def get_total_amount(self) -> float:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute('select sum(quantity) from entries;')
                return cursor.fetchone()[0]

    def get_total_price(self) -> float:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute('select sum(total_price) from entries;')
                return cursor.fetchone()[0]

    def get_total_price_for_date_interval(self, date_interval: DateInterval) -> float:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select sum(total_price)
                    from entries
                    where timestamp >= %s and timestamp < %s
                """, (date_interval.start, date_interval.end))
                return cursor.fetchone()[0]

    def get_amount_at_shop(self, product: str, shop: str, date_interval: DateInterval) -> float:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select sum(quantity)
                    from entries
                    where shop = %s and product = %s and timestamp >= %s and timestamp < %s
                """, (shop, product, date_interval.start, date_interval.end))
                return cursor.fetchone()[0]

    def get_amount(self, product: str, date_interval: DateInterval) -> float:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select sum(quantity)
                    from entries
                    where product = %s and timestamp >= %s and timestamp < %s
                """, (product, date_interval.start, date_interval.end))
                return cursor.fetchone()[0]

    def get_total_price_by_shop(self, date_interval: DateInterval):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select shop, sum(total_price)
                    from entries
                    where timestamp >= %s and timestamp < %s
                    group by shop
                    order by shop
                """, (date_interval.start, date_interval.end))
                return cursor.fetchall()

    def get_top_products_by_2(self, date_interval: DateInterval, limit: int):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select e1.product, e2.product, count(*) as freq
                    from entries e1 join entries e2 on e1.receipt_id = e2.receipt_id
                    where e1.shop = e2.shop and e1.product < e2.product and e1.timestamp >= %s and e1.timestamp < %s
                    group by e1.product, e2.product
                    order by freq desc
                    limit %s
                """, (date_interval.start, date_interval.end, limit))
                return cursor.fetchall()

    def get_top_products_by_3(self, date_interval: DateInterval, limit: int):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select e1.product, e2.product, e3.product, count(*) as freq
                    from entries e1
                        join entries e2 on e1.receipt_id = e2.receipt_id
                        join entries e3 on e2.receipt_id = e3.receipt_id
                    where e1.shop = e2.shop
                        and e2.shop = e3.shop
                        and e1.product < e2.product
                        and e2.product < e3.product
                        and e1.timestamp >= %s and e1.timestamp < %s
                    group by e1.product, e2.product, e3.product
                    order by freq desc
                    limit %s
                """, (date_interval.start, date_interval.end, limit))
                return cursor.fetchall()

    def get_top_products_by_4(self, date_interval: DateInterval, limit: int):
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    select e1.product, e2.product, e3.product, e4.product, count(*) as freq
                    from entries e1
                        join entries e2 on e1.receipt_id = e2.receipt_id
                        join entries e3 on e2.receipt_id = e3.receipt_id
                        join entries e4 on e3.receipt_id = e4.receipt_id
                    where e1.shop = e2.shop
                        and e2.shop = e3.shop
                        and e3.shop = e4.shop
                        and e1.product < e2.product
                        and e2.product < e3.product
                        and e3.product < e4.product
                        and e1.timestamp >= %s and e1.timestamp < %s
                    group by e1.product, e2.product, e3.product, e4.product
                    order by freq desc
                    limit %s
                """, (date_interval.start, date_interval.end, limit))
                return cursor.fetchall()


class ColumnSolution(Solution):
    def __init__(self, session):
        self.session = session

    def populate(self, entries: [Entry]):
        self.session.execute('DROP TABLE IF EXISTS "entries";')
        self.session.execute('DROP TABLE IF EXISTS "products_by_2";')
        self.session.execute('DROP TABLE IF EXISTS "products_by_3";')
        self.session.execute('DROP TABLE IF EXISTS "products_by_4";')

        self.session.execute("""
            create table if not exists "products_by_2"
            (
                timestamp   timestamp,
                product1    text,
                product2    text,
                counter_value counter,
                PRIMARY KEY ((product1, product2), timestamp)
            );
        """)

        self.session.execute("""
                    create table if not exists "products_by_3"
                    (
                        timestamp   timestamp,
                        product1    text,
                        product2    text,
                        product3    text,
                        counter_value counter,
                        PRIMARY KEY ((product1, product2, product3), timestamp)
                    );
                """)

        self.session.execute("""
                    create table if not exists "products_by_4"
                    (
                        timestamp   timestamp,
                        product1    text,
                        product2    text,
                        product3    text,
                        product4    text,
                        counter_value counter,
                        PRIMARY KEY ((product1, product2, product3, product4), timestamp)
                    );
                """)

        with open("column.cql", "r") as f:
            self.session.execute(f.read())

        entries_grouped: defaultdict[tuple[int, str], list[Entry]] = defaultdict(list)
        for entry in entries:
            self.session.execute(
                """insert into "entries" (receipt_id, shop, product, total_price, quantity, timestamp)
                values (%s, %s, %s, %s, %s, %s)""",
                (entry.receipt_id, entry.shop, entry.product, entry.total_price, entry.quantity, entry.timestamp)
            )

            entries_grouped[(entry.receipt_id, entry.shop)].append(entry)

        for entries_group in entries_grouped.values():
            for e1, e2 in product(entries_group, entries_group):
                if e1.product < e2.product:
                    self.session.execute(
                        """update "products_by_2"
                        set counter_value = counter_value + 1
                        where timestamp = %s and product1 = %s and product2 = %s 
                        """,
                        (e1.timestamp, e1.product, e2.product)
                    )

            for e1, e2, e3 in product(entries_group, entries_group, entries_group):
                if e1.product < e2.product < e3.product:
                    self.session.execute(
                        """update "products_by_3"
                        set counter_value = counter_value + 1
                        where timestamp = %s and product1 = %s and product2 = %s and product3 = %s
                        """,
                        (e1.timestamp, e1.product, e2.product, e3.product)
                    )

            for e1, e2, e3, e4 in product(entries_group, entries_group, entries_group, entries_group):
                if e1.product < e2.product < e3.product < e4.product:
                    self.session.execute(
                        """update "products_by_4"
                        set counter_value = counter_value + 1
                        where timestamp = %s and product1 = %s and product2 = %s and product3 = %s and product4 = %s
                        """,
                        (e1.timestamp, e1.product, e2.product, e3.product, e4.product)
                    )

    def get_total_amount(self) -> float:
        return self.session.execute('select sum(quantity) from "entries";').one()[0]

    def get_total_price(self) -> float:
        return self.session.execute('select sum(total_price) from "entries";').one()[0]

    def get_total_price_for_date_interval(self, date_interval: DateInterval) -> float:
        return self.session.execute("""
            select sum(total_price)
            from "entries"
            where timestamp >= %s and timestamp < %s
            allow filtering
        """, (date_interval.start, date_interval.end)).one()[0]

    def get_amount_at_shop(self, product: str, shop: str, date_interval: DateInterval) -> float:
        return self.session.execute("""
            select sum(quantity)
            from "entries"
            where shop = %s and product = %s and timestamp >= %s and timestamp < %s
            allow filtering
        """, (shop, product, date_interval.start, date_interval.end)).one()[0]

    def get_amount(self, product: str, date_interval: DateInterval) -> float:
        return self.session.execute("""
            select sum(quantity)
            from "entries"
            where product = %s and timestamp >= %s and timestamp < %s
            allow filtering
        """, (product, date_interval.start, date_interval.end)).one()[0]

    def get_total_price_by_shop(self, date_interval: DateInterval):
        return list(
            self.session.execute("""
            select shop, sum(total_price)
            from "entries"
            where timestamp >= %s and timestamp < %s
            group by shop
            allow filtering
            """, (date_interval.start, date_interval.end))
        )

    def get_top_products_by_2(self, date_interval: DateInterval, limit: int):
        result = list(
            self.session.execute("""
                select product1, product2, SUM(counter_value) from "products_by_2"
                where timestamp >= %s and timestamp < %s
                group by product1, product2
                ALLOW FILTERING
            """, (date_interval.start, date_interval.end))
        )

        return sorted(result, key=lambda r: r.system_sum_counter_value, reverse=True)[:limit]

    def get_top_products_by_3(self, date_interval: DateInterval, limit: int):
        result = list(
            self.session.execute("""
                        select product1, product2, product3, SUM(counter_value) from "products_by_3"
                        where timestamp >= %s and timestamp < %s
                        group by product1, product2, product3
                        ALLOW FILTERING
                    """, (date_interval.start, date_interval.end))
        )

        return sorted(result, key=lambda r: r.system_sum_counter_value, reverse=True)[:limit]

    def get_top_products_by_4(self, date_interval: DateInterval, limit: int):
        result = list(
            self.session.execute("""
                                select product1, product2, product3, product4, SUM(counter_value) from "products_by_4"
                                where timestamp >= %s and timestamp < %s
                                group by product1, product2, product3, product4
                                ALLOW FILTERING
                            """, (date_interval.start, date_interval.end))
        )

        return sorted(result, key=lambda r: r.system_sum_counter_value, reverse=True)[:limit]
