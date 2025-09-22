#!/usr/bin/env python3
"""
Скрипт для сбора данных о датасетах ASR/TTS с Hugging Face и OpenSLR
Группа 2: Датасеты
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import logging
import re

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('collection_log.txt'),
        logging.StreamHandler()
    ]
)

class DatasetsScraper:
    def __init__(self):
        self.hf_base_url = "https://huggingface.co/api/datasets"
        self.openslr_base_url = "https://openslr.org"
        self.headers = {
            'User-Agent': 'ASR-TTS-Research/1.0'
        }
        self.collected_data = []
        
    def get_huggingface_datasets(self, limit: int = 100) -> List[Dict]:
        """
        Получает датасеты с Hugging Face
        """
        params = {
            'limit': limit,
            'sort': 'downloads',
            'direction': -1
        }
        
        try:
            response = requests.get(self.hf_base_url, params=params, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Ошибка при получении датасетов с Hugging Face: {e}")
            return []
    
    def get_dataset_details(self, dataset_id: str) -> Dict[str, Any]:
        """
        Получает детальную информацию о датасете
        """
        url = f"https://huggingface.co/api/datasets/{dataset_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Ошибка при получении деталей датасета {dataset_id}: {e}")
            return {}
    
    def extract_hf_dataset_data(self, dataset_info: Dict) -> Dict[str, Any]:
        """
        Извлекает нужные данные из информации о датасете Hugging Face
        """
        # Определяем тип датасета
        tags = dataset_info.get('tags', [])
        dataset_type = "unknown"
        if any(tag in tags for tag in ['speech', 'audio', 'asr', 'tts']):
            dataset_type = "speech"
        elif any(tag in tags for tag in ['text', 'nlp']):
            dataset_type = "text"
        
        # Извлекаем языки
        languages = []
        for tag in tags:
            if len(tag) == 2 and tag.islower():  # Код языка ISO
                languages.append(tag)
        
        # Пытаемся извлечь размер из описания
        description = dataset_info.get('cardData', {}).get('description', '')
        size_hours, size_gb = self.extract_size_from_description(description)
        
        return {
            "dataset_name": dataset_info.get('id', ''),
            "description": description,
            "size_hours": size_hours,
            "size_gb": size_gb,
            "language": languages[0] if languages else "unknown",
            "languages": languages,
            "license": dataset_info.get('license', ''),
            "source": "huggingface",
            "url": f"https://huggingface.co/datasets/{dataset_info.get('id', '')}",
            "downloads": dataset_info.get('downloads', 0),
            "created_date": dataset_info.get('created_at', ''),
            "tags": tags,
            "dataset_type": dataset_type
        }
    
    def extract_size_from_description(self, description: str) -> tuple:
        """
        Извлекает размер датасета из описания
        """
        size_hours = None
        size_gb = None
        
        # Поиск часов
        hours_patterns = [
            r'(\d+(?:\.\d+)?)\s*hours?',
            r'(\d+(?:\.\d+)?)\s*ч',
            r'(\d+(?:\.\d+)?)\s*часов?'
        ]
        
        for pattern in hours_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                size_hours = float(match.group(1))
                break
        
        # Поиск гигабайт
        gb_patterns = [
            r'(\d+(?:\.\d+)?)\s*GB',
            r'(\d+(?:\.\d+)?)\s*ГБ',
            r'(\d+(?:\.\d+)?)\s*гигабайт'
        ]
        
        for pattern in gb_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                size_gb = float(match.group(1))
                break
        
        return size_hours, size_gb
    
    def get_openslr_datasets(self) -> List[Dict]:
        """
        Получает информацию о датасетах с OpenSLR
        """
        # OpenSLR не предоставляет API, поэтому используем статический список
        # известных датасетов
        openslr_datasets = [
            {
                "dataset_name": "LibriSpeech",
                "description": "Large-scale English speech recognition corpus",
                "size_hours": 1000,
                "size_gb": 60,
                "language": "en",
                "license": "CC BY 4.0",
                "source": "openslr",
                "url": "https://openslr.org/12/",
                "dataset_type": "speech"
            },
            {
                "dataset_name": "Common Voice",
                "description": "Multilingual speech dataset",
                "size_hours": 14000,
                "size_gb": 1000,
                "language": "multilingual",
                "license": "CC0",
                "source": "openslr",
                "url": "https://openslr.org/70/",
                "dataset_type": "speech"
            },
            {
                "dataset_name": "VoxForge",
                "description": "Accented speech recognition dataset",
                "size_hours": 100,
                "size_gb": 5,
                "language": "en",
                "license": "GPL",
                "source": "openslr",
                "url": "https://openslr.org/7/",
                "dataset_type": "speech"
            },
            {
                "dataset_name": "TED-LIUM",
                "description": "English speech recognition from TED talks",
                "size_hours": 452,
                "size_gb": 25,
                "language": "en",
                "license": "CC BY-NC-ND 3.0",
                "source": "openslr",
                "url": "https://openslr.org/51/",
                "dataset_type": "speech"
            }
        ]
        
        return openslr_datasets
    
    def collect_data(self):
        """
        Основной метод сбора данных
        """
        # Собираем данные с Hugging Face
        logging.info("Собираем датасеты с Hugging Face")
        hf_datasets = self.get_huggingface_datasets(limit=50)
        
        for dataset in hf_datasets:
            dataset_id = dataset.get('id')
            if not dataset_id:
                continue
            
            logging.info(f"Обрабатываем датасет: {dataset_id}")
            
            # Получаем детальную информацию
            dataset_details = self.get_dataset_details(dataset_id)
            if dataset_details:
                extracted_data = self.extract_hf_dataset_data(dataset_details)
                # Фильтруем только речевые датасеты
                if extracted_data.get('dataset_type') == 'speech':
                    self.collected_data.append(extracted_data)
            
            # Пауза между запросами
            time.sleep(1)
        
        # Добавляем данные с OpenSLR
        logging.info("Добавляем датасеты с OpenSLR")
        openslr_datasets = self.get_openslr_datasets()
        self.collected_data.extend(openslr_datasets)
        
        # Сохраняем данные
        self.save_data()
    
    def save_data(self):
        """
        Сохраняет собранные данные в файлы
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем в JSON
        with open(f'datasets_data_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=2)
        
        # Сохраняем сводку
        summary = {
            "total_datasets": len(self.collected_data),
            "huggingface_datasets": len([d for d in self.collected_data if d['source'] == 'huggingface']),
            "openslr_datasets": len([d for d in self.collected_data if d['source'] == 'openslr']),
            "total_hours": sum([d.get('size_hours', 0) for d in self.collected_data if d.get('size_hours')]),
            "total_gb": sum([d.get('size_gb', 0) for d in self.collected_data if d.get('size_gb')]),
            "collection_date": datetime.now().isoformat()
        }
        
        with open(f'collection_summary_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Собрано {len(self.collected_data)} датасетов")
        logging.info(f"Данные сохранены в datasets_data_{timestamp}.json")

def main():
    scraper = DatasetsScraper()
    scraper.collect_data()

if __name__ == "__main__":
    main()
