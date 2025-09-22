#!/usr/bin/env python3
"""
Скрипт для анализа данных ASR/TTS систем
"""

import pandas as pd
import numpy as np
from sqlalchemy import text
from database_config import get_session
from models import System, SystemMetric, BenchmarkResult, Benchmark
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)

class DataAnalyzer:
    def __init__(self):
        self.session = get_session()
    
    def get_systems_overview(self):
        """
        Получает общий обзор систем
        """
        query = text("""
            SELECT 
                COUNT(*) as total_systems,
                COUNT(DISTINCT разработчик) as unique_developers,
                AVG(количество_скачиваний) as avg_downloads,
                MIN(год_первого_релиза) as earliest_year,
                MAX(год_первого_релиза) as latest_year
            FROM systems
            WHERE год_первого_релиза IS NOT NULL
        """)
        
        result = self.session.execute(query).fetchone()
        return {
            'total_systems': result[0],
            'unique_developers': result[1],
            'avg_downloads': float(result[2]) if result[2] else 0,
            'earliest_year': result[3],
            'latest_year': result[4]
        }
    
    def get_top_developers(self, limit=10):
        """
        Получает топ разработчиков по количеству систем
        """
        query = text("""
            SELECT 
                разработчик,
                COUNT(*) as system_count,
                AVG(количество_скачиваний) as avg_downloads,
                SUM(количество_скачиваний) as total_downloads
            FROM systems
            WHERE разработчик IS NOT NULL AND разработчик != ''
            GROUP BY разработчик
            ORDER BY system_count DESC, total_downloads DESC
            LIMIT :limit
        """)
        
        result = self.session.execute(query, {'limit': limit}).fetchall()
        return [
            {
                'developer': row[0],
                'system_count': row[1],
                'avg_downloads': float(row[2]) if row[2] else 0,
                'total_downloads': row[3]
            }
            for row in result
        ]
    
    def get_architecture_distribution(self):
        """
        Получает распределение архитектур
        """
        query = text("""
            SELECT 
                архитектура,
                COUNT(*) as count,
                AVG(количество_скачиваний) as avg_downloads
            FROM systems
            WHERE архитектура IS NOT NULL AND архитектура != 'unknown'
            GROUP BY архитектура
            ORDER BY count DESC
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'architecture': row[0],
                'count': row[1],
                'avg_downloads': float(row[2]) if row[2] else 0
            }
            for row in result
        ]
    
    def get_wer_vs_year_analysis(self):
        """
        Анализ зависимости WER от года публикации для ASR систем
        """
        query = text("""
            SELECT 
                s.год_первого_релиза,
                sm.значение as wer_value,
                sm.датасет,
                s.название as model_name,
                s.архитектура
            FROM systems s
            JOIN system_metrics sm ON s.id = sm.system_id
            WHERE sm.метрика_тип = 'WER' 
                AND s.год_первого_релиза IS NOT NULL
                AND sm.значение IS NOT NULL
                AND sm.значение > 0
            ORDER BY s.год_первого_релиза, sm.значение
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'year': row[0],
                'wer': float(row[1]),
                'dataset': row[2],
                'model_name': row[3],
                'architecture': row[4]
            }
            for row in result
        ]
    
    def get_mos_vs_year_analysis(self):
        """
        Анализ зависимости MOS от года публикации для TTS систем
        """
        query = text("""
            SELECT 
                s.год_первого_релиза,
                sm.значение as mos_value,
                sm.датасет,
                s.название as model_name,
                s.архитектура
            FROM systems s
            JOIN system_metrics sm ON s.id = sm.system_id
            WHERE sm.метрика_тип = 'MOS' 
                AND s.год_первого_релиза IS NOT NULL
                AND sm.значение IS NOT NULL
                AND sm.значение > 0
            ORDER BY s.год_первого_релиза, sm.значение DESC
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'year': row[0],
                'mos': float(row[1]),
                'dataset': row[2],
                'model_name': row[3],
                'architecture': row[4]
            }
            for row in result
        ]
    
    def get_benchmark_analysis(self):
        """
        Анализ результатов бенчмарков
        """
        query = text("""
            SELECT 
                b.название as benchmark_name,
                b.датасет,
                br.метрика_тип,
                br.значение,
                br.ранг,
                s.название as model_name,
                s.архитектура,
                s.год_первого_релиза
            FROM benchmarks b
            JOIN benchmark_results br ON b.id = br.benchmark_id
            JOIN systems s ON br.system_id = s.id
            WHERE br.ранг <= 5  -- Топ-5 результатов
            ORDER BY b.название, br.ранг
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'benchmark_name': row[0],
                'dataset': row[1],
                'metric_type': row[2],
                'value': float(row[3]) if row[3] else 0,
                'rank': row[4],
                'model_name': row[5],
                'architecture': row[6],
                'year': row[7]
            }
            for row in result
        ]
    
    def get_language_distribution(self):
        """
        Получает распределение поддерживаемых языков
        """
        query = text("""
            SELECT 
                поддерживаемые_языки,
                COUNT(*) as count
            FROM systems
            WHERE поддерживаемые_языки IS NOT NULL 
                AND поддерживаемые_языки != ''
            GROUP BY поддерживаемые_языки
            ORDER BY count DESC
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'languages': row[0],
                'count': row[1]
            }
            for row in result
        ]
    
    def get_license_distribution(self):
        """
        Получает распределение лицензий
        """
        query = text("""
            SELECT 
                тип_лицензии,
                COUNT(*) as count,
                AVG(количество_скачиваний) as avg_downloads
            FROM systems
            WHERE тип_лицензии IS NOT NULL AND тип_лицензии != ''
            GROUP BY тип_лицензии
            ORDER BY count DESC
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'license': row[0],
                'count': row[1],
                'avg_downloads': float(row[2]) if row[2] else 0
            }
            for row in result
        ]
    
    def get_dataset_analysis(self):
        """
        Анализ датасетов
        """
        query = text("""
            SELECT 
                название,
                объем_часы,
                объем_гигабайты,
                язык,
                лицензия,
                источник
            FROM datasets
            ORDER BY объем_часы DESC
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'name': row[0],
                'size_hours': float(row[1]) if row[1] else 0,
                'size_gb': float(row[2]) if row[2] else 0,
                'language': row[3],
                'license': row[4],
                'source': row[5]
            }
            for row in result
        ]
    
    def get_yearly_trends(self):
        """
        Получает тренды по годам
        """
        query = text("""
            SELECT 
                год_первого_релиза,
                COUNT(*) as systems_count,
                AVG(количество_скачиваний) as avg_downloads,
                COUNT(DISTINCT разработчик) as unique_developers
            FROM systems
            WHERE год_первого_релиза IS NOT NULL
                AND год_первого_релиза >= 2010
            GROUP BY год_первого_релиза
            ORDER BY год_первого_релиза
        """)
        
        result = self.session.execute(query).fetchall()
        return [
            {
                'year': row[0],
                'systems_count': row[1],
                'avg_downloads': float(row[2]) if row[2] else 0,
                'unique_developers': row[3]
            }
            for row in result
        ]
    
    def run_full_analysis(self):
        """
        Запускает полный анализ данных
        """
        logging.info("Начинаем полный анализ данных")
        
        analysis_results = {
            'overview': self.get_systems_overview(),
            'top_developers': self.get_top_developers(),
            'architecture_distribution': self.get_architecture_distribution(),
            'wer_vs_year': self.get_wer_vs_year_analysis(),
            'mos_vs_year': self.get_mos_vs_year_analysis(),
            'benchmark_analysis': self.get_benchmark_analysis(),
            'language_distribution': self.get_language_distribution(),
            'license_distribution': self.get_license_distribution(),
            'dataset_analysis': self.get_dataset_analysis(),
            'yearly_trends': self.get_yearly_trends()
        }
        
        logging.info("Анализ данных завершен")
        return analysis_results

def main():
    """
    Основная функция для запуска анализа
    """
    analyzer = DataAnalyzer()
    results = analyzer.run_full_analysis()
    
    # Выводим основные результаты
    print("\n=== ОБЗОР СИСТЕМ ===")
    overview = results['overview']
    print(f"Всего систем: {overview['total_systems']}")
    print(f"Уникальных разработчиков: {overview['unique_developers']}")
    print(f"Среднее количество скачиваний: {overview['avg_downloads']:.0f}")
    print(f"Годы: {overview['earliest_year']} - {overview['latest_year']}")
    
    print("\n=== ТОП РАЗРАБОТЧИКИ ===")
    for dev in results['top_developers'][:5]:
        print(f"{dev['developer']}: {dev['system_count']} систем, {dev['total_downloads']} скачиваний")
    
    print("\n=== РАСПРЕДЕЛЕНИЕ АРХИТЕКТУР ===")
    for arch in results['architecture_distribution'][:5]:
        print(f"{arch['architecture']}: {arch['count']} систем")
    
    print("\n=== АНАЛИЗ WER ПО ГОДАМ ===")
    wer_data = results['wer_vs_year']
    if wer_data:
        df = pd.DataFrame(wer_data)
        yearly_wer = df.groupby('year')['wer'].agg(['mean', 'min', 'count']).round(3)
        print(yearly_wer.head(10))
    
    print("\n=== АНАЛИЗ MOS ПО ГОДАМ ===")
    mos_data = results['mos_vs_year']
    if mos_data:
        df = pd.DataFrame(mos_data)
        yearly_mos = df.groupby('year')['mos'].agg(['mean', 'max', 'count']).round(3)
        print(yearly_mos.head(10))

if __name__ == "__main__":
    main()
