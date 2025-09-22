#!/usr/bin/env python3
"""
SQLAlchemy модели для ASR/TTS систем
"""

from sqlalchemy import Column, Integer, String, Text, DECIMAL, TIMESTAMP, DATE, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database_config import Base

# Промежуточные таблицы для связей многие-ко-многим
system_vocabulary_types = Table(
    'system_vocabulary_types',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('system_id', Integer, ForeignKey('systems.id', ondelete='CASCADE')),
    Column('vocabulary_type_id', Integer, ForeignKey('vocabulary_types.id', ondelete='CASCADE'))
)

system_functional_purposes = Table(
    'system_functional_purposes',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('system_id', Integer, ForeignKey('systems.id', ondelete='CASCADE')),
    Column('functional_purpose_id', Integer, ForeignKey('functional_purposes.id', ondelete='CASCADE'))
)

class VocabularyType(Base):
    __tablename__ = 'vocabulary_types'
    
    id = Column(Integer, primary_key=True)
    тип = Column(String(50), unique=True, nullable=False)
    описание = Column(Text)
    диапазон_слов = Column(String(100))
    
    # Связи
    systems = relationship("System", secondary=system_vocabulary_types, back_populates="vocabulary_types")

class FunctionalPurpose(Base):
    __tablename__ = 'functional_purposes'
    
    id = Column(Integer, primary_key=True)
    назначение = Column(String(100), unique=True, nullable=False)
    описание = Column(Text)
    
    # Связи
    systems = relationship("System", secondary=system_functional_purposes, back_populates="functional_purposes")

class System(Base):
    __tablename__ = 'systems'
    
    id = Column(Integer, primary_key=True)
    название = Column(String(255), nullable=False)
    разработчик = Column(String(255))
    год_первого_релиза = Column(Integer)
    описание = Column(Text)
    ссылка_на_источник = Column(String(500))
    тип_лицензии = Column(String(100))
    архитектура = Column(String(100))
    поддерживаемые_языки = Column(Text)
    количество_скачиваний = Column(Integer, default=0)
    дата_создания = Column(TIMESTAMP, default=func.current_timestamp())
    дата_обновления = Column(TIMESTAMP, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    # Связи
    vocabulary_types = relationship("VocabularyType", secondary=system_vocabulary_types, back_populates="systems")
    functional_purposes = relationship("FunctionalPurpose", secondary=system_functional_purposes, back_populates="systems")
    metrics = relationship("SystemMetric", back_populates="system", cascade="all, delete-orphan")
    papers = relationship("SystemPaper", back_populates="system", cascade="all, delete-orphan")
    benchmark_results = relationship("BenchmarkResult", back_populates="system", cascade="all, delete-orphan")

class SystemMetric(Base):
    __tablename__ = 'system_metrics'
    
    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey('systems.id', ondelete='CASCADE'))
    метрика_тип = Column(String(50), nullable=False)
    значение = Column(DECIMAL(10, 4))
    датасет = Column(String(255))
    язык = Column(String(50))
    дата_измерения = Column(DATE)
    
    # Связи
    system = relationship("System", back_populates="metrics")

class SystemPaper(Base):
    __tablename__ = 'system_papers'
    
    id = Column(Integer, primary_key=True)
    system_id = Column(Integer, ForeignKey('systems.id', ondelete='CASCADE'))
    название_статьи = Column(String(500), nullable=False)
    ссылка_arxiv = Column(String(500))
    год_публикации = Column(Integer)
    авторы = Column(Text)
    
    # Связи
    system = relationship("System", back_populates="papers")

class Dataset(Base):
    __tablename__ = 'datasets'
    
    id = Column(Integer, primary_key=True)
    название = Column(String(255), nullable=False)
    описание = Column(Text)
    объем_часы = Column(DECIMAL(10, 2))
    объем_гигабайты = Column(DECIMAL(10, 2))
    язык = Column(String(100))
    лицензия = Column(String(100))
    источник = Column(String(100))
    ссылка = Column(String(500))
    дата_создания = Column(TIMESTAMP, default=func.current_timestamp())

class Benchmark(Base):
    __tablename__ = 'benchmarks'
    
    id = Column(Integer, primary_key=True)
    название = Column(String(255), nullable=False)
    задачи = Column(Text)
    датасет = Column(String(255))
    описание = Column(Text)
    ссылка = Column(String(500))
    источник = Column(String(100))
    дата_создания = Column(TIMESTAMP, default=func.current_timestamp())
    
    # Связи
    results = relationship("BenchmarkResult", back_populates="benchmark", cascade="all, delete-orphan")

class BenchmarkResult(Base):
    __tablename__ = 'benchmark_results'
    
    id = Column(Integer, primary_key=True)
    benchmark_id = Column(Integer, ForeignKey('benchmarks.id', ondelete='CASCADE'))
    system_id = Column(Integer, ForeignKey('systems.id', ondelete='CASCADE'))
    ранг = Column(Integer)
    метрика_тип = Column(String(50), nullable=False)
    значение = Column(DECIMAL(10, 4))
    датасет_раздел = Column(String(50))
    ссылка_на_статью = Column(String(500))
    ссылка_на_код = Column(String(500))
    дата_отправки = Column(DATE)
    
    # Связи
    benchmark = relationship("Benchmark", back_populates="results")
    system = relationship("System", back_populates="benchmark_results")
