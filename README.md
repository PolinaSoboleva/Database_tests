# Database_tests

Для загрузки необходимых модулей используйте файл requirements.txt (pip install -r requirements.txt).

Для запуска на разных стендах и бд необходимо в файле config.yaml добавить подключение, далее в файле test_database.py в классе PostgresDB поленять значение на нужную конфигурацию.
Пример, конфигурация 'prod' -> self.db_config = load_db_config('prod').

