# Конфигурация базы данных
# Скопируйте этот файл в config.py и настройте под ваши нужды

# Выберите тип базы данных: 'postgresql' или 'sqlite'
DB_TYPE = 'sqlite'

# Настройки PostgreSQL (если используется)
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'asr_tts_systems'
DB_USER = 'postgres'
DB_PASSWORD = 'your_password'

# Настройки SQLite (если используется)
SQLITE_DB = 'asr_tts_systems.db'

# Настройки логирования
LOG_LEVEL = 'INFO'
