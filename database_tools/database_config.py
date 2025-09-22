#!/usr/bin/env python3
"""
Конфигурация для работы с базой данных
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройки базы данных
DATABASE_CONFIG = {
    'postgresql': {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'asr_tts_systems'),
        'username': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password')
    },
    'sqlite': {
        'database': os.getenv('SQLITE_DB', 'asr_tts_systems.db')
    }
}

# Выбор типа базы данных
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # 'postgresql' или 'sqlite'

def get_database_url():
    """
    Возвращает URL для подключения к базе данных
    """
    if DB_TYPE == 'postgresql':
        config = DATABASE_CONFIG['postgresql']
        return f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
    else:
        config = DATABASE_CONFIG['sqlite']
        return f"sqlite:///{config['database']}"

def create_database_engine():
    """
    Создает движок базы данных
    """
    database_url = get_database_url()
    return create_engine(database_url, echo=False)

def get_session():
    """
    Создает сессию базы данных
    """
    engine = create_database_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

# Базовый класс для моделей
Base = declarative_base()

# Глобальные переменные
engine = create_database_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_database():
    """
    Инициализирует базу данных (создает таблицы)
    """
    Base.metadata.create_all(bind=engine)
    print(f"База данных инициализирована: {DB_TYPE}")

def get_db():
    """
    Генератор для получения сессии базы данных
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
