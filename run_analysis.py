#!/usr/bin/env python3
"""
Основной скрипт для запуска фазы реализации и анализа
"""

import os
import sys
import logging
from datetime import datetime

# Добавляем пути к модулям
sys.path.append('database_tools')
sys.path.append('analysis')
sys.path.append('visualization')

from database_tools.database_config import init_database
from database_tools.data_loader import DataLoader
from analysis.data_analysis import DataAnalyzer
from visualization.visualization import DataVisualizer

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'analysis_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'),
        logging.StreamHandler()
    ]
)

def main():
    """
    Основная функция для запуска полного анализа
    """
    logging.info("=== НАЧАЛО ФАЗЫ РЕАЛИЗАЦИИ И АНАЛИЗА ===")
    
    try:
        # Шаг 1: Инициализация базы данных
        logging.info("Шаг 1: Инициализация базы данных")
        init_database()
        
        # Шаг 2: Загрузка данных
        logging.info("Шаг 2: Загрузка данных в базу")
        loader = DataLoader()
        loader.load_all_data()
        
        # Шаг 3: Анализ данных
        logging.info("Шаг 3: Анализ данных")
        analyzer = DataAnalyzer()
        results = analyzer.run_full_analysis()
        
        # Шаг 4: Создание визуализаций
        logging.info("Шаг 4: Создание визуализаций")
        visualizer = DataVisualizer()
        visualizer.create_all_visualizations()
        
        # Шаг 5: Вывод результатов
        logging.info("Шаг 5: Вывод результатов")
        print_analysis_summary(results)
        
        logging.info("=== ФАЗА РЕАЛИЗАЦИИ И АНАЛИЗА ЗАВЕРШЕНА УСПЕШНО ===")
        
    except Exception as e:
        logging.error(f"Ошибка в процессе анализа: {e}")
        raise

def print_analysis_summary(results):
    """
    Выводит сводку результатов анализа
    """
    print("\n" + "="*60)
    print("СВОДКА РЕЗУЛЬТАТОВ АНАЛИЗА")
    print("="*60)
    
    # Общий обзор
    overview = results['overview']
    print(f"\n📊 ОБЩИЙ ОБЗОР:")
    print(f"   • Всего систем: {overview['total_systems']}")
    print(f"   • Уникальных разработчиков: {overview['unique_developers']}")
    print(f"   • Средние скачивания: {overview['avg_downloads']:.0f}")
    print(f"   • Период: {overview['earliest_year']} - {overview['latest_year']}")
    
    # Топ разработчики
    if results['top_developers']:
        print(f"\n👨‍💻 ТОП РАЗРАБОТЧИКИ:")
        for i, dev in enumerate(results['top_developers'][:5], 1):
            print(f"   {i}. {dev['developer']}: {dev['system_count']} систем")
    
    # Архитектуры
    if results['architecture_distribution']:
        print(f"\n🏗️ АРХИТЕКТУРЫ:")
        for i, arch in enumerate(results['architecture_distribution'][:5], 1):
            print(f"   {i}. {arch['architecture']}: {arch['count']} систем")
    
    # Анализ WER
    if results['wer_vs_year']:
        wer_data = results['wer_vs_year']
        best_wer = min(wer_data, key=lambda x: x['wer'])
        print(f"\n🎯 ЛУЧШИЙ WER:")
        print(f"   • Модель: {best_wer['model_name']}")
        print(f"   • WER: {best_wer['wer']:.2f}%")
        print(f"   • Год: {best_wer['year']}")
        print(f"   • Датасет: {best_wer['dataset']}")
    
    # Анализ MOS
    if results['mos_vs_year']:
        mos_data = results['mos_vs_year']
        best_mos = max(mos_data, key=lambda x: x['mos'])
        print(f"\n🎵 ЛУЧШИЙ MOS:")
        print(f"   • Модель: {best_mos['model_name']}")
        print(f"   • MOS: {best_mos['mos']:.2f}")
        print(f"   • Год: {best_mos['year']}")
        print(f"   • Датасет: {best_mos['dataset']}")
    
    # Бенчмарки
    if results['benchmark_analysis']:
        print(f"\n🏆 БЕНЧМАРКИ:")
        benchmark_count = len(set(r['benchmark_name'] for r in results['benchmark_analysis']))
        print(f"   • Количество бенчмарков: {benchmark_count}")
        print(f"   • Всего результатов: {len(results['benchmark_analysis'])}")
    
    # Датасеты
    if results['dataset_analysis']:
        print(f"\n📚 ДАТАСЕТЫ:")
        total_hours = sum(d.get('size_hours', 0) for d in results['dataset_analysis'])
        total_gb = sum(d.get('size_gb', 0) for d in results['dataset_analysis'])
        print(f"   • Количество датасетов: {len(results['dataset_analysis'])}")
        print(f"   • Общий объем: {total_hours:.0f} часов, {total_gb:.0f} ГБ")
    
    print(f"\n📁 СОЗДАННЫЕ ФАЙЛЫ:")
    print(f"   • Графики: wer_vs_year.png, mos_vs_year.png, architecture_distribution.png")
    print(f"   • Интерактивные графики: interactive_wer.html")
    print(f"   • База данных: asr_tts_systems.db (SQLite)")
    print(f"   • Лог анализа: analysis_log_*.txt")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
