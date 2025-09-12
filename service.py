import sqlite3
from flight import Flight

import psycopg2
from psycopg2 import sql


class FlightService:
    def __init__(self,
                 database: str = "",
                 host: str = "",
                 user: str = "",
                 password: str = "",
                 port: str = ""):
        self.connection_parameters = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.init_db()

    def get_connection(self):
        return psycopg2.connect(**self.connection_parameters)

    def init_db(self):
        """Иницилизация таблиц"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS flights(
                         id SERIAL PRIMARY KEY,
                         plane VARCHAR(100) NOT NULL,
                         price DECIMAL(10,2) NOT NULL
                         )
                ''')
            conn.commit()

    def create_flight(self, flight: Flight):
        """Добавление рейса"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO flights
                         (plane,price)
                         VALUES (%s,%s)
                ''', (flight.plane, flight.price))
            conn.commit()
            return cursor.rowcount > 0

    def get_all(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM flights ORDER BY id")
            rows = cursor.fetchall()

            flights = []
            for row in rows:
                flights.append(Flight(
                    row[0],
                    row[1],
                    row[2]
                ))
            return flights

    def get_by_id(self, flight_id: int):
        """Получить рейс по идентификатору"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM flights WHERE id = %s", (flight_id,))
            row = cursor.fetchone()

            if row:
                return Flight(
                    row[0],
                    row[1],
                    row[2]
                )
        return None

    def update_flight(self, flight: Flight):
        """Изменить существующий рейс.
            Если рейса не существует, ничего не делать."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE flights
                SET price = %s, plane = %s
                WHERE id = %s
                ''', (flight.price, flight.plane, flight.id))
            conn.commit()
            return cursor.rowcount > 0

    def delete_flight(self, flight_id: int):
        """Удалить существующий рейс.
            Если рейса не существует, ничего не делать."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM flights WHERE id = %s
                ''', (flight_id,))
            conn.commit()
            return cursor.rowcount > 0