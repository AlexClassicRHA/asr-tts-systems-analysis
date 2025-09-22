#!/usr/bin/env python3
"""
Скрипт для сбора данных о научных статьях по ASR/TTS
Группа 3: Научные статьи
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any
import logging
import re
from urllib.parse import quote

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('collection_log.txt'),
        logging.StreamHandler()
    ]
)

class PapersScraper:
    def __init__(self):
        self.arxiv_base_url = "http://export.arxiv.org/api/query"
        self.headers = {
            'User-Agent': 'ASR-TTS-Research/1.0'
        }
        self.collected_data = []
        
        # Ключевые слова для поиска
        self.search_terms = [
            "speech recognition",
            "text to speech", 
            "speech synthesis",
            "voice cloning",
            "automatic speech recognition",
            "neural text to speech",
            "end-to-end speech recognition"
        ]
    
    def search_arxiv(self, query: str, max_results: int = 50) -> List[Dict]:
        """
        Ищет статьи на arXiv
        """
        params = {
            'search_query': f'all:{query}',
            'start': 0,
            'max_results': max_results,
            'sortBy': 'relevance',
            'sortOrder': 'descending'
        }
        
        try:
            response = requests.get(self.arxiv_base_url, params=params, headers=self.headers)
            response.raise_for_status()
            return self.parse_arxiv_response(response.text)
        except requests.RequestException as e:
            logging.error(f"Ошибка при поиске на arXiv для запроса '{query}': {e}")
            return []
    
    def parse_arxiv_response(self, xml_content: str) -> List[Dict]:
        """
        Парсит XML ответ от arXiv
        """
        import xml.etree.ElementTree as ET
        
        papers = []
        try:
            root = ET.fromstring(xml_content)
            
            # Находим все записи
            for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                paper_data = self.extract_paper_data(entry)
                if paper_data:
                    papers.append(paper_data)
                    
        except ET.ParseError as e:
            logging.error(f"Ошибка парсинга XML: {e}")
        
        return papers
    
    def extract_paper_data(self, entry) -> Dict[str, Any]:
        """
        Извлекает данные о статье из XML элемента
        """
        try:
            # Основная информация
            title = entry.find('.//{http://www.w3.org/2005/Atom}title').text.strip()
            summary = entry.find('.//{http://www.w3.org/2005/Atom}summary').text.strip()
            
            # Авторы
            authors = []
            for author in entry.findall('.//{http://www.w3.org/2005/Atom}author'):
                name = author.find('.//{http://www.w3.org/2005/Atom}name')
                if name is not None:
                    authors.append(name.text.strip())
            
            # Дата публикации
            published = entry.find('.//{http://www.w3.org/2005/Atom}published')
            publication_year = None
            if published is not None:
                publication_year = int(published.text[:4])
            
            # Ссылка на arXiv
            arxiv_link = None
            for link in entry.findall('.//{http://www.w3.org/2005/Atom}link'):
                if link.get('type') == 'text/html':
                    arxiv_link = link.get('href')
                    break
            
            # ID статьи
            paper_id = entry.find('.//{http://www.w3.org/2005/Atom}id').text.split('/')[-1]
            
            # Извлекаем метрики из текста
            metrics = self.extract_metrics_from_text(summary + " " + title)
            
            # Определяем тип системы
            system_type = self.determine_system_type(title, summary)
            
            return {
                "paper_title": title,
                "arxiv_link": arxiv_link,
                "arxiv_id": paper_id,
                "publication_year": publication_year,
                "authors": authors,
                "summary": summary,
                "system_type": system_type,
                "metrics": metrics,
                "model_name": self.extract_model_name(title, summary)
            }
            
        except Exception as e:
            logging.error(f"Ошибка при извлечении данных статьи: {e}")
            return None
    
    def extract_metrics_from_text(self, text: str) -> List[Dict]:
        """
        Извлекает метрики из текста статьи
        """
        metrics = []
        text_lower = text.lower()
        
        # Паттерны для поиска метрик
        metric_patterns = {
            'WER': [
                r'wer[:\s]*(\d+\.?\d*)\s*%?',
                r'word error rate[:\s]*(\d+\.?\d*)\s*%?',
                r'(\d+\.?\d*)\s*%?\s*wer'
            ],
            'CER': [
                r'cer[:\s]*(\d+\.?\d*)\s*%?',
                r'character error rate[:\s]*(\d+\.?\d*)\s*%?'
            ],
            'MOS': [
                r'mos[:\s]*(\d+\.?\d*)',
                r'mean opinion score[:\s]*(\d+\.?\d*)'
            ],
            'BLEU': [
                r'bleu[:\s]*(\d+\.?\d*)',
                r'bleu score[:\s]*(\d+\.?\d*)'
            ]
        }
        
        # Поиск датасетов
        dataset_patterns = [
            r'librispeech',
            r'common voice',
            r'voxforge',
            r'ted-lium',
            r'wsj',
            r'switchboard'
        ]
        
        for metric_type, patterns in metric_patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, text_lower)
                for match in matches:
                    value = float(match.group(1))
                    
                    # Определяем датасет
                    dataset = "unknown"
                    for dataset_pattern in dataset_patterns:
                        if re.search(dataset_pattern, text_lower):
                            dataset = dataset_pattern.replace('-', ' ').title()
                            break
                    
                    metrics.append({
                        "type": metric_type,
                        "value": value,
                        "dataset": dataset,
                        "language": "en"  # По умолчанию английский
                    })
        
        return metrics
    
    def determine_system_type(self, title: str, summary: str) -> str:
        """
        Определяет тип системы (ASR, TTS, etc.)
        """
        text = (title + " " + summary).lower()
        
        if any(term in text for term in ['speech recognition', 'asr', 'automatic speech']):
            return 'ASR'
        elif any(term in text for term in ['text to speech', 'tts', 'speech synthesis', 'voice synthesis']):
            return 'TTS'
        elif any(term in text for term in ['voice cloning', 'voice conversion']):
            return 'Voice Cloning'
        else:
            return 'Unknown'
    
    def extract_model_name(self, title: str, summary: str) -> str:
        """
        Извлекает название модели из заголовка или описания
        """
        # Ищем названия моделей в заголовке
        title_lower = title.lower()
        
        # Известные названия моделей
        known_models = [
            'whisper', 'wav2vec', 'tacotron', 'fastspeech', 'tacotron2',
            'waveglow', 'melgan', 'hifigan', 'conformer', 'transformer',
            'listen attend and spell', 'deep speech', 'jasper', 'quartznet'
        ]
        
        for model in known_models:
            if model in title_lower:
                return model.title()
        
        # Если не найдено, берем первые слова заголовка
        words = title.split()[:3]
        return ' '.join(words)
    
    def collect_data(self):
        """
        Основной метод сбора данных
        """
        for search_term in self.search_terms:
            logging.info(f"Ищем статьи по запросу: {search_term}")
            
            papers = self.search_arxiv(search_term, max_results=20)
            
            for paper in papers:
                if paper and paper not in self.collected_data:
                    self.collected_data.append(paper)
            
            # Пауза между запросами
            time.sleep(2)
        
        # Удаляем дубликаты по arXiv ID
        unique_papers = []
        seen_ids = set()
        
        for paper in self.collected_data:
            paper_id = paper.get('arxiv_id')
            if paper_id and paper_id not in seen_ids:
                seen_ids.add(paper_id)
                unique_papers.append(paper)
        
        self.collected_data = unique_papers
        
        # Сохраняем данные
        self.save_data()
    
    def save_data(self):
        """
        Сохраняет собранные данные в файлы
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем в JSON
        with open(f'papers_data_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=2)
        
        # Сохраняем сводку
        summary = {
            "total_papers": len(self.collected_data),
            "asr_papers": len([p for p in self.collected_data if p.get('system_type') == 'ASR']),
            "tts_papers": len([p for p in self.collected_data if p.get('system_type') == 'TTS']),
            "voice_cloning_papers": len([p for p in self.collected_data if p.get('system_type') == 'Voice Cloning']),
            "papers_with_metrics": len([p for p in self.collected_data if p.get('metrics')]),
            "collection_date": datetime.now().isoformat()
        }
        
        with open(f'collection_summary_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        logging.info(f"Собрано {len(self.collected_data)} статей")
        logging.info(f"Данные сохранены в papers_data_{timestamp}.json")

def main():
    scraper = PapersScraper()
    scraper.collect_data()

if __name__ == "__main__":
    main()
