import unittest
import datetime
import math
from db_config import load_db_config

from parameterized import parameterized
import psycopg2
from psycopg2 import errors


class PostgresDB:
    def __init__(self):
        # Загрузить конфигурацию
        self.db_config = load_db_config('local')


class TestDatabaseInsert(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_config = PostgresDB().db_config

        cls.conn = psycopg2.connect(
            dbname=cls.db_config['dbname'],
            user=cls.db_config['user'],
            password=cls.db_config['password'],
            host=cls.db_config['host'],
            port=cls.db_config['port']
        )

        cls.cursor = cls.conn.cursor()

    @classmethod
    def tearDownClass(cls):
        cls.cursor.close()
        cls.conn.close()

    def setUp(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS people (index BIGINT NOT NULL PRIMARY KEY, 
        name VARCHAR(50) NOT NULL, dateofbirth DATE NOT NULL);""")
        self.conn.commit()

    def tearDown(self):
        self.cursor.execute("DROP TABLE IF EXISTS people;")
        self.conn.commit()

    @parameterized.expand([
        (1, 'John', datetime.date(2000, 1, 1)),
        (0, 'Полина', datetime.date(1925, 1, 1)),
        (-1, 'Анна-Мария', datetime.date(1924, 1, 1)),
        (9223372036854775807, 'Марта Кристина', datetime.date(1923, 1, 1)),
        (9223372036854775806, 'По', datetime.date(2006, 1, 1)),
        (-9223372036854775808, 'J', datetime.date(2006, 6, 4)),
        (-9223372036854775807, '', datetime.date(2006, 6, 1)),
        (3, 'Frank64', datetime.date(2024, 8, 1)),
        (4, '55564', datetime.date(1823, 1, 1)),
        (5, '!”№;%:?*()_+', datetime.date(834, 1, 1)),
        (16, '55564', datetime.date(1, 1, 1)),
        (17, '55564', datetime.date(4, 1, 1)),
        (6, 'Павел''da', datetime.date(1970, 1, 1)),
        (7, 'Павел””пират', datetime.date(2000, 1, 1)),
        (8, 'Sam"pi', datetime.date(2000, 1, 1)),
        (17, '55564', datetime.date(4, 1, 1)),
        (9, 'Иванов Иван Иванович Ковалев Родилин Родионов Мак', datetime.date(1900, 1, 1)),
    ])
    def test_insert_data(self, index, name, dateofbirth):
        insert_query = "INSERT INTO people VALUES (%s, %s, %s) RETURNING index;"
        self.cursor.execute(insert_query, (index, name, dateofbirth,))
        inserted_id = self.cursor.fetchone()[0]
        self.conn.commit()

        select_query = "SELECT * FROM people WHERE index = %s;"
        self.cursor.execute(select_query, (inserted_id,))
        result = self.cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], index)
        self.assertEqual(result[1], name)
        self.assertEqual(result[2], dateofbirth)

    @parameterized.expand([
        (10, 9.8, 'Иванов Иван Иванович Ковалев Родилин Родионов Макс', datetime.date(2000, 1, 1)),
        (26, math.log(255232223333), 'Иванов Иван Иванович', datetime.date(2000, 1, 1)),
        (123, '123', 'Kate', datetime.date(2011, 7, 1)),
    ])
    def test_insert_data_with_expecting_index(self, expect_ind, index, name, dateofbirth):
        insert_query = "INSERT INTO people VALUES (%s, %s, %s) RETURNING index;"
        self.cursor.execute(insert_query, (index, name, dateofbirth,))
        inserted_id = self.cursor.fetchone()[0]
        self.conn.commit()

        select_query = "SELECT * FROM people WHERE index = %s;"
        self.cursor.execute(select_query, (inserted_id,))
        result = self.cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], expect_ind)
        self.assertEqual(result[1], name)
        self.assertEqual(result[2], dateofbirth)

    @parameterized.expand([
        ('7', 18, 7, datetime.date(2000, 1, 1)),
    ])
    def test_insert_data_with_expecting_name(self, expect_name, index, name, dateofbirth):
        insert_query = "INSERT INTO people VALUES (%s, %s, %s) RETURNING index;"
        self.cursor.execute(insert_query, (index, name, dateofbirth,))
        inserted_id = self.cursor.fetchone()[0]
        self.conn.commit()

        select_query = "SELECT * FROM people WHERE index = %s;"
        self.cursor.execute(select_query, (inserted_id,))
        result = self.cursor.fetchone()

        self.assertIsNotNone(result)
        self.assertEqual(result[0], index)
        self.assertEqual(result[1], expect_name)
        self.assertEqual(result[2], dateofbirth)

    def test_insert_invalid_data_with_none(self):
        test_cases = [
            {"data": {"index": '123', "name": None, "dateofbirth": datetime.date(2000, 1, 1)},
             "expected_exception": errors.NotNullViolation},  # name is None
            {"data": {"index": None, "name": 'Kate', "dateofbirth": datetime.date(2000, 1, 1)},
             "expected_exception": errors.NotNullViolation},  # index is None
            {"data": {"index": 1234, "name": 'Kate', "dateofbirth": None},
             "expected_exception": errors.NotNullViolation},
            # dateofbirth is None
            {"data": {"index": 1234, "name": None, "dateofbirth": None}, "expected_exception": errors.NotNullViolation},
            # name and dateofbirth is None
        ]

        for case in test_cases:
            with self.subTest(case=case):
                with self.assertRaises(case["expected_exception"]):
                    insert_query = "INSERT INTO people VALUES (%s, %s, %s);"
                    self.cursor.execute(insert_query,
                                        (case["data"]["index"], case["data"]["name"], case["data"]["dateofbirth"]))
                self.conn.commit()

    @parameterized.expand([
        (9223372036854775808, 'John', datetime.date(2000, 1, 1), errors.NumericValueOutOfRange),
        (-9223372036854775809, 'Полина', datetime.date(2000, 1, 1), errors.NumericValueOutOfRange),
        ('один', 'Анна-Мария', datetime.date(2011, 7, 1), errors.InvalidTextRepresentation),
        ('№1', 'Марта Кристина', datetime.date(2011, 7, 1), errors.InvalidTextRepresentation),
        (9.8, {2: 4}, datetime.date(2011, 7, 1), errors.ProgrammingError),
        (9.8, 'Иванов Иван Иванович Ковалев Родилин Родионов Макс1', datetime.date(2011, 7, 1),
         errors.StringDataRightTruncation),
    ])
    def test_insert_invalid_data(self, index, name, dateofbirth, expected_exception):
        with self.assertRaises(expected_exception):
            self.cursor.execute("INSERT INTO people VALUES (%s, %s, %s);", (index, name, dateofbirth))
        self.conn.commit()

    @parameterized.expand([
        (1, '1', 'John', datetime.date(2000, 1, 1), errors.UniqueViolation),
        (9, 9, 'Полина', datetime.date(2000, 1, 1), errors.UniqueViolation),
        (0, '000', 'Анна-Мария', datetime.date(2011, 7, 1), errors.UniqueViolation),
    ])
    def test_insert_dublicate_data(self, index_for_insert_first, index, name, dateofbirth, expected_exception):
        insert_query = "INSERT INTO people VALUES (%s, %s, %s)"
        self.cursor.execute(insert_query, (index_for_insert_first, name, dateofbirth,))
        self.conn.commit()
        with self.assertRaises(expected_exception):
            self.cursor.execute("INSERT INTO people VALUES (%s, %s, %s);", (index, name, dateofbirth))
        self.conn.commit()


class TestDatabaseUpdateDelete(unittest.TestCase):

    @classmethod
    def setUp(cls):
        cls.db_config = PostgresDB().db_config

        cls.connection = psycopg2.connect(
            dbname=cls.db_config['dbname'],
            user=cls.db_config['user'],
            password=cls.db_config['password'],
            host=cls.db_config['host'],
            port=cls.db_config['port']
        )

        cls.cursor = cls.connection.cursor()

        cls.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS people 
            (index BIGINT NOT NULL PRIMARY KEY, 
            name VARCHAR(50) NOT NULL, 
            dateofbirth DATE NOT NULL);
            """)

        cls.cursor.execute("INSERT INTO people (index, name, dateofbirth) VALUES (%s, %s, %s);",
                           (1, 'Jane', datetime.date(2000, 1, 1)))
        cls.cursor.execute("INSERT INTO people (index, name, dateofbirth) VALUES (%s, %s, %s);",
                           (2, 'Jane', datetime.date(2020, 1, 1)))
        cls.connection.commit()

    @classmethod
    def tearDown(cls):
        cls.cursor.execute("DROP TABLE IF EXISTS people;")
        cls.connection.commit()
        cls.cursor.close()
        cls.connection.close()

    @parameterized.expand([
        (1, 3, "Johnny", datetime.date(2000, 1, 1)),
        (2, 4, "Janny", datetime.date(2000, 1, 1)),
    ])
    def test_update_all_columns_with_index(self, old_index, new_index, new_name, new_dateofbirth):
        self.cursor.execute("UPDATE people SET index = %s, name = %s, dateofbirth = %s WHERE index = %s",
                            (new_index, new_name, new_dateofbirth, old_index))
        self.connection.commit()

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (new_index,))
        result = self.cursor.fetchone()

        self.assertEqual(result, (new_index, new_name, new_dateofbirth))

    @parameterized.expand([
        (4, 5),
    ])
    def test_update_non_existent_index(self, old_index, new_index):
        self.cursor.execute("UPDATE people SET index = %s WHERE index = %s",
                            (new_index, old_index))
        self.connection.commit()

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (new_index,))
        id = self.cursor.fetchone()

        self.assertIsNone(id)

    @parameterized.expand([
        (1, 2, "John", datetime.date(2000, 1, 1), errors.UniqueViolation),
        (2, "twenty", 'Jonny', datetime.date(2000, 1, 1), errors.InvalidTextRepresentation),
        (2, '2000-01-01', 'Jonny', datetime.date(2000, 1, 1), errors.InvalidTextRepresentation),
    ])
    def test_update_invalid_index(self, old_index, new_index, new_name, new_dateofbirth, expected_exception):
        with self.assertRaises(expected_exception):
            self.cursor.execute("UPDATE people SET index = %s, name = %s, dateofbirth = %s WHERE index = %s",
                                (new_index, new_name, new_dateofbirth, old_index))
        self.connection.commit()

    @parameterized.expand([
        (1, 'Jane', "John", datetime.date(2000, 1, 1), errors.UniqueViolation),
    ])
    def test_update_invalid_name_with_two_lines(self, new_index, old_name, new_name, new_dateofbirth,
                                                expected_exception):
        with self.assertRaises(expected_exception):
            self.cursor.execute("UPDATE people SET index = %s, name = %s, dateofbirth = %s WHERE name = %s",
                                (new_index, new_name, new_dateofbirth, old_name))
        self.connection.commit()

    def test_update_invalid_column(self):
        with self.assertRaises(errors.UndefinedColumn):
            self.cursor.execute("UPDATE people SET inex = %s WHERE name = %s",
                                ("default_id", "default_name"))
        self.connection.commit()

    def test_update_invalid_table(self):
        with self.assertRaises(errors.UndefinedTable):
            self.cursor.execute("UPDATE people1 SET inex = %s WHERE name = %s",
                                ("default_id", "default_name"))
        self.connection.commit()

    @parameterized.expand([
        ("name", "Jane"),
        ("index", 2),
    ])
    def test_delete_valid_data(self, column, identifier):
        if column == 'name':
            self.cursor.execute("DELETE FROM people WHERE name = %s RETURNING index", (identifier,))
        elif column == 'index':
            self.cursor.execute("DELETE FROM people WHERE index = %s RETURNING index", (identifier,))
        else:
            raise ValueError

        deleted_id = self.cursor.fetchone()
        self.connection.commit()

        self.assertIsNotNone(deleted_id)

    @parameterized.expand([
        (4),
    ])
    def test_delete_non_existent_index(self, index):
        self.cursor.execute("DELETE FROM people WHERE index = %s",
                            (index,))
        self.connection.commit()

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (index,))
        id = self.cursor.fetchone()

        self.assertIsNone(id)

    @parameterized.expand([
        ("more", datetime.date(2005, 1, 1)),
        ("less", datetime.date(2001, 1, 1)),
    ])
    def test_delete_valid_data_with_date(self, condition, identifier):
        if condition == 'more':
            self.cursor.execute("DELETE FROM people WHERE dateofbirth > %s RETURNING index", (identifier,))
        elif condition == 'less':
            self.cursor.execute("DELETE FROM people WHERE dateofbirth < %s RETURNING index", (identifier,))
        else:
            raise ValueError

        deleted_id = self.cursor.fetchone()
        self.connection.commit()

        self.assertIsNotNone(deleted_id)

    def test_delete_undefined_column(self):
        with self.assertRaises(errors.UndefinedColumn):
            self.cursor.execute("DELETE FROM people WHERE id = 25")
        self.connection.commit()

    def test_delete_undefined_table(self):
        with self.assertRaises(errors.UndefinedTable):
            self.cursor.execute("DELETE FROM people1")
        self.connection.commit()


class TestTableModifications(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_config = PostgresDB().db_config

        cls.connection = psycopg2.connect(
            dbname=cls.db_config['dbname'],
            user=cls.db_config['user'],
            password=cls.db_config['password'],
            host=cls.db_config['host'],
            port=cls.db_config['port']
        )

        cls.cursor = cls.connection.cursor()

        cls.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS people 
            (index BIGINT NOT NULL PRIMARY KEY, 
             FirstName VARCHAR(50) NOT NULL, 
             FamilyName VARCHAR(50) NOT NULL, 
             DateOfBirth DATE NOT NULL,
             PlaceOfBirth VARCHAR(100), 
             Occupation VARCHAR(100), 
             Hobby VARCHAR(300));
            """)

        cls.connection.commit()

    @classmethod
    def tearDownClass(cls):
        cls.cursor.execute("DROP TABLE IF EXISTS people;")
        cls.connection.commit()
        cls.cursor.close()
        cls.connection.close()

    @parameterized.expand([
        'Persons', 'Персона', 'J', 'Persons_3$'])
    def test_rename_table(self, table_name):
        try:
            self.cursor.execute("ALTER TABLE People RENAME TO " + table_name + ";")
            self.connection.commit()
            self.cursor.execute("SELECT * FROM " + table_name + ";")
            self.connection.commit()
        except Exception as e:
            self.fail(f"Renaming table failed: {e}")
        finally:
            self.cursor.execute("ALTER TABLE " + table_name + " RENAME TO People;")
            self.connection.commit()

    def test_rename_table_on_operation_word(self):
        with self.assertRaises(errors.SyntaxError):
            self.cursor.execute("ALTER TABLE People RENAME TO GROUP;")
        self.connection.commit()

    @parameterized.expand([
        'GivenName', 'Имя', 'J', 'Name_3$'])
    def test_rename_column(self, column_name):
        try:
            self.cursor.execute("ALTER TABLE People RENAME COLUMN FirstName TO " + column_name + " ;")
            self.connection.commit()
            self.cursor.execute("SELECT " + column_name + " FROM People;")
            self.connection.commit()
        except Exception as e:
            self.fail(f"Renaming column failed: {e}")
        finally:
            self.cursor.execute("ALTER TABLE People RENAME COLUMN " + column_name + " TO FirstName;")
            self.connection.commit()

    @parameterized.expand([
        'Email', 'Почта', 'E', 'Email_3$'])
    def test_add_column(self, column_name):
        try:
            self.cursor.execute("ALTER TABLE People ADD COLUMN " + column_name + " VARCHAR(255);")
            self.connection.commit()
            self.cursor.execute("SELECT " + column_name + " FROM People;")
            self.connection.commit()
        except Exception as e:
            self.fail(f"Adding column failed: {e}")
        finally:
            self.cursor.execute("ALTER TABLE People DROP COLUMN " + column_name + ";")
            self.connection.commit()

    def test_drop_column(self):
        try:
            self.cursor.execute("ALTER TABLE People ADD COLUMN TempColumn VARCHAR(255);")
            self.connection.commit()
            self.cursor.execute("ALTER TABLE People DROP COLUMN TempColumn;")
            self.connection.commit()
        except Exception as e:
            self.fail(f"Dropping column failed: {e}")

        with self.assertRaises(errors.UndefinedColumn):
            self.cursor.execute("SELECT TempColumn FROM People;")
        self.connection.commit()

    def test_alter_column_type(self):
        try:
            self.cursor.execute("ALTER TABLE People ALTER COLUMN DateOfBirth TYPE TIMESTAMP;")
            self.connection.commit()
        except Exception as e:
            self.fail(f"Altering column type failed: {e}")
        finally:
            self.cursor.execute("ALTER TABLE People ALTER COLUMN DateOfBirth TYPE DATE;")
            self.connection.commit()

    def test_rename_nonexistent_table(self):
        with self.assertRaises(psycopg2.Error):
            self.cursor.execute("ALTER TABLE UnknownTable RENAME TO NewTable;")
        self.connection.commit()

    def test_rename_nonexistent_column(self):
        with self.assertRaises(psycopg2.Error):
            self.cursor.execute("ALTER TABLE People RENAME COLUMN UnknownColumn TO NewColumn;")
        self.connection.commit()

    def test_add_existing_column(self):
        with self.assertRaises(psycopg2.Error):
            self.cursor.execute("ALTER TABLE People ADD COLUMN FirstName VARCHAR(255);")
        self.connection.commit()

    def test_drop_nonexistent_column(self):
        with self.assertRaises(psycopg2.Error):
            self.cursor.execute("ALTER TABLE People DROP COLUMN UnknownColumn;")
        self.connection.commit()

    def test_alter_column_type_incompatible(self):
        with self.assertRaises(psycopg2.Error):
            self.cursor.execute("INSERT INTO People VALUES (%s, %s, %s, %s);",
                                (1, 'Jane', 'Kolin', datetime.date(2020, 1, 1)))
            self.connection.commit()
            self.cursor.execute("ALTER TABLE People ALTER COLUMN DateOfBirth TYPE BIGINT;")
        self.connection.commit()


if __name__ == "__main__":
    unittest.main()
