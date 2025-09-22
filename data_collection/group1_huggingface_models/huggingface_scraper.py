#!/usr/bin/env python3
"""
Скрипт для сбора данных о моделях ASR/TTS с Hugging Face
Группа 1: Модели с Hugging Face
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('collection_log.txt'),
        logging.StreamHandler()
    ]
)

class HuggingFaceScraper:
    def __init__(self):
        self.base_url = "https://huggingface.co/api/models"
        self.headers = {
            'User-Agent': 'ASR-TTS-Research/1.0'
        }
        self.collected_data = []
        
    def get_models_by_pipeline(self, pipeline_tag: str, limit: int = 100) -> List[Dict]:
        """
        Получает модели по типу пайплайна
        """
        params = {
            'pipeline_tag': pipeline_tag,
            'limit': limit,
            'sort': 'downloads',
            'direction': -1
        }
        
        try:
            response = requests.get(self.base_url, params=params, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Ошибка при получении данных для {pipeline_tag}: {e}")
            return []
    
    def get_model_details(self, model_id: str) -> Dict[str, Any]:
        """
        Получает детальную информацию о модели
        """
        url = f"https://huggingface.co/api/models/{model_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Ошибка при получении деталей модели {model_id}: {e}")
            return {}
    
    def extract_model_data(self, model_info: Dict) -> Dict[str, Any]:
        """
        Извлекает нужные данные из информации о модели
        """
        # Определяем тип системы на основе pipeline_tag
        pipeline_tags = model_info.get('pipeline_tag', [])
        system_type = "unknown"
        if 'automatic-speech-recognition' in pipeline_tags:
            system_type = "ASR"
        elif 'text-to-speech' in pipeline_tags:
            system_type = "TTS"
        elif 'audio-to-audio' in pipeline_tags:
            system_type = "Audio-to-Audio"
        
        # Извлекаем языки из тегов
        languages = []
        tags = model_info.get('tags', [])
        for tag in tags:
            if len(tag) == 2 and tag.islower():  # Код языка ISO
                languages.append(tag)
        
        # Определяем архитектуру из тегов
        architecture = "unknown"
        for tag in tags:
            if any(arch in tag.lower() for arch in ['transformer', 'whisper', 'wav2vec', 'tacotron', 'fastspeech']):
                architecture = tag
                break
        
        return {
            "model_name": model_info.get('id', ''),
            "author_organization": model_info.get('author', ''),
            "system_type": system_type,
            "architecture": architecture,
            "downloads": model_info.get('downloads', 0),
            "languages": languages,
            "license": model_info.get('license', ''),
            "created_date": model_info.get('created_at', ''),
            "last_modified": model_info.get('last_modified', ''),
            "description": model_info.get('cardData', {}).get('description', ''),
            "pipeline_tags": pipeline_tags,
            "tags": tags,
            "model_url": f"https://huggingface.co/{model_info.get('id', '')}",
            "papers": self.extract_papers(model_info)
        }
    
    def extract_papers(self, model_info: Dict) -> List[Dict]:
        """
        Извлекает информацию о научных статьях
        """
        papers = []
        card_data = model_info.get('cardData', {})
        
        # Ищем ссылки на arXiv в описании
        description = card_data.get('description', '')
        if 'arxiv.org' in description:
            # Простое извлечение ссылок на arXiv
            import re
            arxiv_links = re.findall(r'https://arxiv\.org/abs/\d+\.\d+', description)
            for link in arxiv_links:
                papers.append({
                    "arxiv_link": link,
                    "source": "description"
                })
        
        return papers
    
    def collect_data(self):
        """
        Основной метод сбора данных
        """
        pipeline_tags = [
            'automatic-speech-recognition',
            'text-to-speech', 
            'audio-to-audio'
        ]
        
        for pipeline_tag in pipeline_tags:
            logging.info(f"Собираем данные для {pipeline_tag}")
            
            models = self.get_models_by_pipeline(pipeline_tag, limit=50)
            
            for model in models:
                model_id = model.get('id')
                if not model_id:
                    continue
                
                logging.info(f"Обрабатываем модель: {model_id}")
                
                # Получаем детальную информацию
                model_details = self.get_model_details(model_id)
                if model_details:
                    extracted_data = self.extract_model_data(model_details)
                    self.collected_data.append(extracted_data)
                
                # Пауза между запросами
                time.sleep(1)
        
        # Сохраняем данные
        self.save_data()
    
    def save_data(self):
        """
        Сохраняет собранные данные в файлы
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем в JSON
        with open(f'models_data_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=2)
        
        # Сохраняем сводку
        summary = {
            "total_models": len(self.collected_data),
            "asr_models": len([m for m in self.collected_data if m['system_type'] == 'ASR']),
            "tts_models": len([m for m in self.collected_data if m['system_type'] == 'TTS']),
            "audio_to_audio_models": len([m for m in self.collected_data if m['system_type'] == 'Audio-to-Audio']),
            "collection_date": datetime.now().isoformat()
        }
        
        with open(f'collection_summary_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Собрано {len(self.collected_data)} моделей")
        logging.info(f"Данные сохранены в models_data_{timestamp}.json")

def main():
    scraper = HuggingFaceScraper()
    scraper.collect_data()

if __name__ == "__main__":
    main()
