#!/usr/bin/env python3
"""
Скрипт для загрузки собранных данных в базу данных
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any
import logging
from tqdm import tqdm

from database_config import get_session, init_database
from models import (
    System, VocabularyType, FunctionalPurpose, SystemMetric, 
    SystemPaper, Dataset, Benchmark, BenchmarkResult
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataLoader:
    def __init__(self):
        self.session = get_session()
        self.vocabulary_types = {}
        self.functional_purposes = {}
        
    def load_vocabulary_types(self):
        """
        Загружает типы словарей
        """
        vocabulary_data = [
            {'тип': 'малый', 'описание': 'Системы с ограниченным словарем', 'диапазон_слов': 'до 1000 слов'},
            {'тип': 'средний', 'описание': 'Системы со средним словарем', 'диапазон_слов': '1000-10000 слов'},
            {'тип': 'большой (LVCSR)', 'описание': 'Large Vocabulary Continuous Speech Recognition', 'диапазон_слов': '10000+ слов'}
        ]
        
        for data in vocabulary_data:
            existing = self.session.query(VocabularyType).filter_by(тип=data['тип']).first()
            if not existing:
                vocab_type = VocabularyType(**data)
                self.session.add(vocab_type)
                self.session.commit()
                self.vocabulary_types[data['тип']] = vocab_type
            else:
                self.vocabulary_types[data['тип']] = existing
        
        logging.info(f"Загружено {len(self.vocabulary_types)} типов словарей")
    
    def load_functional_purposes(self):
        """
        Загружает функциональные назначения
        """
        purpose_data = [
            {'назначение': 'командная', 'описание': 'Распознавание голосовых команд'},
            {'назначение': 'диктовка', 'описание': 'Преобразование речи в текст для диктовки'},
            {'назначение': 'понимание (SLU)', 'описание': 'Spoken Language Understanding - понимание смысла речи'},
            {'назначение': 'диалоговая', 'описание': 'Диалоговые системы и чат-боты'}
        ]
        
        for data in purpose_data:
            existing = self.session.query(FunctionalPurpose).filter_by(назначение=data['назначение']).first()
            if not existing:
                purpose = FunctionalPurpose(**data)
                self.session.add(purpose)
                self.session.commit()
                self.functional_purposes[data['назначение']] = purpose
            else:
                self.functional_purposes[data['назначение']] = existing
        
        logging.info(f"Загружено {len(self.functional_purposes)} функциональных назначений")
    
    def load_systems_from_json(self, file_path: str):
        """
        Загружает системы из JSON файла
        """
        if not os.path.exists(file_path):
            logging.error(f"Файл не найден: {file_path}")
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in tqdm(data, desc="Загрузка систем"):
            try:
                # Создаем систему
                system_data = {
                    'название': item.get('model_name', ''),
                    'разработчик': item.get('author_organization', ''),
                    'описание': item.get('description', ''),
                    'ссылка_на_источник': item.get('model_url', ''),
                    'тип_лицензии': item.get('license', ''),
                    'архитектура': item.get('architecture', ''),
                    'поддерживаемые_языки': ', '.join(item.get('languages', [])),
                    'количество_скачиваний': item.get('downloads', 0)
                }
                
                # Парсим дату создания
                created_date = item.get('created_date', '')
                if created_date:
                    try:
                        system_data['год_первого_релиза'] = int(created_date[:4])
                    except:
                        pass
                
                system = System(**system_data)
                self.session.add(system)
                self.session.flush()  # Получаем ID
                
                # Добавляем типы словарей
                self._add_vocabulary_types(system, item)
                
                # Добавляем функциональные назначения
                self._add_functional_purposes(system, item)
                
                # Добавляем метрики
                self._add_metrics(system, item)
                
                # Добавляем статьи
                self._add_papers(system, item)
                
            except Exception as e:
                logging.error(f"Ошибка при загрузке системы {item.get('model_name', 'Unknown')}: {e}")
                continue
        
        self.session.commit()
        logging.info(f"Загружено систем из файла: {file_path}")
    
    def _add_vocabulary_types(self, system: System, item: Dict):
        """
        Добавляет типы словарей к системе
        """
        # Определяем тип словаря на основе архитектуры или других признаков
        architecture = item.get('architecture', '').lower()
        if 'whisper' in architecture or 'wav2vec' in architecture:
            vocab_type = self.vocabulary_types.get('большой (LVCSR)')
        elif 'tacotron' in architecture or 'fastspeech' in architecture:
            vocab_type = self.vocabulary_types.get('средний')
        else:
            vocab_type = self.vocabulary_types.get('средний')  # По умолчанию
        
        if vocab_type:
            system.vocabulary_types.append(vocab_type)
    
    def _add_functional_purposes(self, system: System, item: Dict):
        """
        Добавляет функциональные назначения к системе
        """
        system_type = item.get('system_type', '').lower()
        pipeline_tags = item.get('pipeline_tags', [])
        
        if 'asr' in system_type or 'automatic-speech-recognition' in pipeline_tags:
            purpose = self.functional_purposes.get('диктовка')
            if purpose:
                system.functional_purposes.append(purpose)
        
        if 'tts' in system_type or 'text-to-speech' in pipeline_tags:
            purpose = self.functional_purposes.get('диалоговая')
            if purpose:
                system.functional_purposes.append(purpose)
    
    def _add_metrics(self, system: System, item: Dict):
        """
        Добавляет метрики к системе
        """
        # Здесь можно добавить логику для извлечения метрик из данных
        pass
    
    def _add_papers(self, system: System, item: Dict):
        """
        Добавляет статьи к системе
        """
        papers = item.get('papers', [])
        for paper in papers:
            paper_data = {
                'название_статьи': f"Paper for {system.название}",
                'ссылка_arxiv': paper.get('arxiv_link', ''),
                'авторы': 'Unknown'
            }
            paper_obj = SystemPaper(**paper_data)
            system.papers.append(paper_obj)
    
    def load_datasets_from_json(self, file_path: str):
        """
        Загружает датасеты из JSON файла
        """
        if not os.path.exists(file_path):
            logging.error(f"Файл не найден: {file_path}")
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in tqdm(data, desc="Загрузка датасетов"):
            try:
                dataset_data = {
                    'название': item.get('dataset_name', ''),
                    'описание': item.get('description', ''),
                    'объем_часы': item.get('size_hours'),
                    'объем_гигабайты': item.get('size_gb'),
                    'язык': item.get('language', ''),
                    'лицензия': item.get('license', ''),
                    'источник': item.get('source', ''),
                    'ссылка': item.get('url', '')
                }
                
                dataset = Dataset(**dataset_data)
                self.session.add(dataset)
                
            except Exception as e:
                logging.error(f"Ошибка при загрузке датасета {item.get('dataset_name', 'Unknown')}: {e}")
                continue
        
        self.session.commit()
        logging.info(f"Загружено датасетов из файла: {file_path}")
    
    def load_benchmarks_from_json(self, file_path: str):
        """
        Загружает бенчмарки из JSON файла
        """
        if not os.path.exists(file_path):
            logging.error(f"Файл не найден: {file_path}")
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in tqdm(data, desc="Загрузка бенчмарков"):
            try:
                # Создаем бенчмарк
                benchmark_data = {
                    'название': item.get('benchmark_name', ''),
                    'задачи': ', '.join(item.get('tasks', [])),
                    'датасет': item.get('dataset', ''),
                    'описание': item.get('description', ''),
                    'ссылка': item.get('url', ''),
                    'источник': item.get('source', '')
                }
                
                benchmark = Benchmark(**benchmark_data)
                self.session.add(benchmark)
                self.session.flush()  # Получаем ID
                
                # Добавляем результаты
                results = item.get('results', [])
                for result in results:
                    self._add_benchmark_result(benchmark, result)
                
            except Exception as e:
                logging.error(f"Ошибка при загрузке бенчмарка {item.get('benchmark_name', 'Unknown')}: {e}")
                continue
        
        self.session.commit()
        logging.info(f"Загружено бенчмарков из файла: {file_path}")
    
    def _add_benchmark_result(self, benchmark: Benchmark, result: Dict):
        """
        Добавляет результат бенчмарка
        """
        # Находим систему по названию
        model_name = result.get('model_name', '')
        system = self.session.query(System).filter_by(название=model_name).first()
        
        if not system:
            # Создаем систему если не найдена
            system_data = {
                'название': model_name,
                'разработчик': 'Unknown',
                'описание': f'System from benchmark {benchmark.название}'
            }
            system = System(**system_data)
            self.session.add(system)
            self.session.flush()
        
        # Добавляем метрики
        metrics = result.get('metrics', [])
        for metric in metrics:
            result_data = {
                'benchmark_id': benchmark.id,
                'system_id': system.id,
                'ранг': result.get('rank', 0),
                'метрика_тип': metric.get('type', ''),
                'значение': metric.get('value', 0),
                'датасет_раздел': metric.get('dataset_split', 'test'),
                'ссылка_на_статью': result.get('paper_link', ''),
                'ссылка_на_код': result.get('code_link', '')
            }
            
            benchmark_result = BenchmarkResult(**result_data)
            self.session.add(benchmark_result)
    
    def load_all_data(self, data_dir: str = "../data_collection"):
        """
        Загружает все данные из папки сбора данных
        """
        logging.info("Начинаем загрузку всех данных")
        
        # Инициализируем базу данных
        init_database()
        
        # Загружаем справочные данные
        self.load_vocabulary_types()
        self.load_functional_purposes()
        
        # Загружаем данные по группам
        groups = [
            ("group1_huggingface_models", "models_data_*.json", self.load_systems_from_json),
            ("group2_datasets", "datasets_data_*.json", self.load_datasets_from_json),
            ("group3_papers", "papers_data_*.json", self.load_papers_from_json),
            ("group4_benchmarks", "benchmarks_data_*.json", self.load_benchmarks_from_json)
        ]
        
        for group_name, file_pattern, loader_func in groups:
            group_dir = os.path.join(data_dir, group_name)
            if os.path.exists(group_dir):
                # Ищем файлы данных
                import glob
                files = glob.glob(os.path.join(group_dir, file_pattern))
                for file_path in files:
                    logging.info(f"Загружаем данные из {file_path}")
                    loader_func(file_path)
        
        logging.info("Загрузка данных завершена")
    
    def load_papers_from_json(self, file_path: str):
        """
        Загружает статьи из JSON файла
        """
        if not os.path.exists(file_path):
            logging.error(f"Файл не найден: {file_path}")
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in tqdm(data, desc="Загрузка статей"):
            try:
                model_name = item.get('model_name', '')
                system = self.session.query(System).filter_by(название=model_name).first()
                
                if not system:
                    # Создаем систему если не найдена
                    system_data = {
                        'название': model_name,
                        'разработчик': 'Unknown',
                        'описание': f'System from paper {item.get("paper_title", "")}'
                    }
                    system = System(**system_data)
                    self.session.add(system)
                    self.session.flush()
                
                # Добавляем статью
                paper_data = {
                    'system_id': system.id,
                    'название_статьи': item.get('paper_title', ''),
                    'ссылка_arxiv': item.get('arxiv_link', ''),
                    'год_публикации': item.get('publication_year'),
                    'авторы': ', '.join(item.get('authors', []))
                }
                
                paper = SystemPaper(**paper_data)
                self.session.add(paper)
                
                # Добавляем метрики из статьи
                metrics = item.get('metrics', [])
                for metric in metrics:
                    metric_data = {
                        'system_id': system.id,
                        'метрика_тип': metric.get('type', ''),
                        'значение': metric.get('value', 0),
                        'датасет': metric.get('dataset', ''),
                        'язык': metric.get('language', '')
                    }
                    
                    system_metric = SystemMetric(**metric_data)
                    self.session.add(system_metric)
                
            except Exception as e:
                logging.error(f"Ошибка при загрузке статьи {item.get('paper_title', 'Unknown')}: {e}")
                continue
        
        self.session.commit()
        logging.info(f"Загружено статей из файла: {file_path}")

def main():
    """
    Основная функция для загрузки данных
    """
    loader = DataLoader()
    loader.load_all_data()

if __name__ == "__main__":
    main()
