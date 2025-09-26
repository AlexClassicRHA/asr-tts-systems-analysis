#!/usr/bin/env python3
"""
Скрипт для сбора данных о бенчмарках ASR с Hugging Face Leaderboard
Группа 4: Бенчмарки и лидерборды
"""

import pandas as pd
import json
from datetime import datetime
from typing import List, Dict, Any
import logging
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('huggingface_leaderboard_log.txt'),
        logging.StreamHandler()
    ]
)

class HuggingFaceLeaderboardScraper:
    def __init__(self):
        self.collected_data = []
        self.backup_csv_file = "models_benchmarks.csv"
    
    def extract_benchmark_data(self) -> List[Dict[str, Any]]:
        """Основной метод извлечения данных бенчмарка"""
        try:
            # Пытаемся использовать Selenium для парсинга
            return self._parse_with_selenium()
        except Exception as e:
            logging.error(f"Ошибка при парсинге через Selenium: {e}")
            logging.info(f"Пытаемся загрузить данные из резервного файла: {self.backup_csv_file}")
            return self._load_from_backup_csv()
    
    def _parse_with_selenium(self) -> List[Dict[str, Any]]:
        """Парсинг данных с использованием Selenium"""
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from webdriver_manager.chrome import ChromeDriverManager
            import time
            
            # Настройка Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Настройка и инициализация WebDriver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            try:
                logging.info("Открытие страницы Hugging Face Leaderboard...")
                driver.get("https://huggingface.co/spaces/hf-audio/open_asr_leaderboard")
                
                # Ожидание загрузки страницы
                time.sleep(5)
                
                # Поиск и переключение на iframe
                logging.info("Поиск iframe...")
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "iframe"))
                )
                
                iframes = driver.find_elements(By.TAG_NAME, "iframe")
                logging.info(f"Найдено iframe: {len(iframes)}")
                
                # Переключение на основной iframe с контентом
                driver.switch_to.frame(iframes[0])
                logging.info("Переключились на iframe")
                
                # Ожидание загрузки таблицы внутри iframe
                logging.info("Ожидание загрузки таблицы...")
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "svelte-1xyl3gk"))
                )
                logging.info("Таблица найдена")
                
                # Дадим дополнительное время для загрузки данных
                time.sleep(5)
                
                # Извлечение заголовков таблицы
                headers = self._extract_headers_selenium(driver)
                
                # Извлечение данных таблицы
                table_data = self._extract_table_content_selenium(driver, headers)
                
                # Преобразование в формат бенчмарков
                benchmarks = self._convert_to_benchmark_format(table_data, headers)
                
                return benchmarks
                
            finally:
                try:
                    driver.switch_to.default_content()
                except:
                    pass
                driver.quit()
                
        except Exception as e:
            logging.error(f"Ошибка в Selenium парсере: {e}")
            raise
    
    def _extract_headers_selenium(self, driver) -> List[str]:
        """Извлечение заголовков таблицы через Selenium"""
        headers = []
        try:
            header_elements = driver.find_elements(By.XPATH, "//table//thead//th")
            
            for header in header_elements:
                header_text = header.text.strip()
                if header_text:
                    headers.append(header_text)
            
            logging.info(f"Найдено заголовков: {len(headers)}")
        except Exception as e:
            logging.error(f"Ошибка при извлечении заголовков: {e}")
            # Используем заранее известные заголовки
            headers = ["model", "Average WER ⬇️", "RTFx ⬆️️", "License", "AMI", "Earnings22", 
                      "Gigaspeech", "LS Clean", "LS Other", "SPGISpeech", "Tedlium", "Voxpopuli"]
        
        return headers
    
    def _extract_table_content_selenium(self, driver, headers: List[str]) -> List[Dict[str, Any]]:
        """Извлечение содержимого таблицы через Selenium"""
        table_container = driver.find_element(By.CLASS_NAME, "svelte-1xyl3gk")
        data = []
        seen_models = set()
        scroll_step = 100
        max_scrolls = 100
        scroll_count = 0
        no_new_models_count = 0
        
        while scroll_count < max_scrolls and no_new_models_count < 5:
            rows = driver.find_elements(By.XPATH, "//table//tbody//tr")
            current_row_count = len(rows)
            logging.info(f"Найдено строк: {current_row_count}")
            
            new_models_found = 0
            for row in rows:
                try:
                    cols = row.find_elements(By.TAG_NAME, "td")
                    if cols and len(cols) == len(headers):
                        row_data = [col.text.strip() for col in cols]
                        model_name = row_data[0]
                        
                        if model_name and model_name not in seen_models:
                            row_dict = dict(zip(headers, row_data))
                            data.append(row_dict)
                            seen_models.add(model_name)
                            new_models_found += 1
                            logging.info(f"Добавлена модель {len(data)}: {model_name}")
                except Exception as e:
                    logging.error(f"Ошибка при обработке строки: {e}")
            
            if new_models_found == 0:
                no_new_models_count += 1
                logging.info(f"Новых моделей не найдено. Попытка {no_new_models_count}/5")
            else:
                no_new_models_count = 0
            
            if len(data) >= 60:
                logging.info(f"Собрано достаточно данных ({len(data)} моделей), завершаем прокрутку")
                break
            
            # Прокрутка таблицы
            current_scroll_position = driver.execute_script("return arguments[0].scrollTop", table_container)
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1]", 
                                 table_container, scroll_step)
            time.sleep(0.5)
            
            new_scroll_position = driver.execute_script("return arguments[0].scrollTop", table_container)
            if new_scroll_position == current_scroll_position:
                logging.info("Достигнут конец таблицы")
                break
            
            scroll_count += 1
        
        return data
    
    def _load_from_backup_csv(self) -> List[Dict[str, Any]]:
        """Загрузка данных из резервного CSV-файла"""
        try:
            if not os.path.exists(self.backup_csv_file):
                logging.error(f"Резервный файл {self.backup_csv_file} не найден")
                return []
            
            logging.info(f"Чтение данных из резервного файла: {self.backup_csv_file}")
            df = pd.read_csv(self.backup_csv_file)
            logging.info(f"Загружено {len(df)} строк из CSV")
            
            # Преобразуем DataFrame в список словарей
            table_data = df.to_dict('records')
            
            # Получаем заголовки из DataFrame
            headers = df.columns.tolist()
            
            # Преобразование в формат бенчмарков
            benchmarks = self._convert_to_benchmark_format(table_data, headers)
            
            return benchmarks
            
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных из CSV: {e}")
            return []
    
    def _convert_to_benchmark_format(self, table_data: List[Dict], headers: List[str]) -> List[Dict[str, Any]]:
        """Преобразование данных таблицы в формат бенчмарков"""
        
        # Создаем один основной бенчмарк для Hugging Face Leaderboard
        benchmark = {
            "benchmark_name": "Hugging Face ASR Leaderboard",
            "tasks": ["automatic-speech-recognition"],
            "dataset": "Multiple (AMI, Earnings22, GigaSpeech, LibriSpeech, etc.)",
            "url": "https://huggingface.co/spaces/hf-audio/open_asr_leaderboard",
            "description": "Comprehensive benchmark for automatic speech recognition models evaluated on multiple datasets",
            "source": "huggingface",
            "results": []
        }
        
        # Преобразуем каждую строку таблицы в результат бенчмарка
        for i, row_data in enumerate(table_data):
            try:
                result = self._create_benchmark_result(row_data, i + 1)
                if result:
                    benchmark["results"].append(result)
            except Exception as e:
                logging.error(f"Ошибка при преобразовании строки {i}: {e}")
        
        return [benchmark]
    
    def _create_benchmark_result(self, row_data: Dict, rank: int) -> Dict[str, Any]:
        """Создание результата бенчмарка из данных строки"""
        # Определяем имя модели в зависимости от структуры данных
        model_name = ""
        if 'model' in row_data:
            model_name = row_data.get('model', '')
        elif 'model_name' in row_data:
            model_name = row_data.get('model_name', '')
        elif 'Model' in row_data:
            model_name = row_data.get('Model', '')
        
        if not model_name:
            return None
        
        # Извлекаем метрики
        metrics = []
        
        # Average WER (может быть под разными именами)
        avg_wer_keys = ['Average WER ⬇️', 'Average WER', 'WER', 'average_wer']
        avg_wer = self._get_first_existing_value(row_data, avg_wer_keys)
        if avg_wer and str(avg_wer) != "-" and str(avg_wer) != "nan":
            metrics.append({
                "type": "Average WER",
                "value": self._parse_metric_value(avg_wer),
                "dataset_split": "average"
            })
        
        # RTFx
        rtfx_keys = ['RTFx ⬆️️', 'RTFx', 'rtfx']
        rtfx = self._get_first_existing_value(row_data, rtfx_keys)
        if rtfx and str(rtfx) != "-" and str(rtfx) != "nan":
            metrics.append({
                "type": "RTFx",
                "value": self._parse_metric_value(rtfx),
                "dataset_split": "average"
            })
        
        # Метрики для отдельных датасетов
        dataset_mapping = {
            'AMI': 'AMI',
            'Earnings22': 'Earnings22', 
            'Gigaspeech': 'Gigaspeech',
            'LS Clean': 'LS Clean',
            'LS Other': 'LS Other',
            'SPGISpeech': 'SPGISpeech',
            'Tedlium': 'Tedlium',
            'Voxpopuli': 'Voxpopuli'
        }
        
        for dataset_key, dataset_name in dataset_mapping.items():
            value = row_data.get(dataset_key, "")
            if value and str(value) != "-" and str(value) != "nan":
                metrics.append({
                    "type": "WER",
                    "value": self._parse_metric_value(value),
                    "dataset_split": dataset_name.lower().replace(" ", "_")
                })
        
        # Генерируем URL модели
        model_url = self._generate_model_url(model_name)
        
        return {
            "model_name": model_name,
            "rank": rank,
            "metrics": metrics,
            "paper_link": "",
            "code_link": model_url,
            "submission_date": ""
        }
    
    def _get_first_existing_value(self, row_data: Dict, keys: List[str]) -> Any:
        """Получение первого существующего значения из списка ключей"""
        for key in keys:
            if key in row_data:
                value = row_data[key]
                if value and str(value) != "nan":
                    return value
        return None
    
    def _parse_metric_value(self, value_str: Any) -> float:
        """Парсинг значения метрики из строки или числа"""
        try:
            if isinstance(value_str, (int, float)):
                return float(value_str)
            
            # Убираем проценты и пробелы
            cleaned = str(value_str).replace('%', '').replace(' ', '').strip()
            if cleaned == '-' or cleaned == 'nan' or cleaned == '':
                return 0.0
            return float(cleaned)
        except ValueError:
            logging.warning(f"Не удалось распарсить значение метрики: {value_str}")
            return 0.0
    
    def _generate_model_url(self, model_name: str) -> str:
        """Генерация URL модели на Hugging Face"""
        if model_name and '/' in model_name:
            return f"https://huggingface.co/{model_name}"
        return ""
    
    def collect_data(self):
        """
        Основной метод сбора данных
        """
        logging.info("Запуск сбора данных с Hugging Face ASR Leaderboard")
        
        benchmark_data = self.extract_benchmark_data()
        self.collected_data = benchmark_data
        
        # Сохранение данных
        self.save_data()
        
        return benchmark_data
    
    def save_data(self):
        """
        Сохраняет собранные данные в файлы
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем в JSON
        output_file = f'huggingface_leaderboard_{timestamp}.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, ensure_ascii=False, indent=2)
        
        # Сохраняем сводку
        total_results = sum(len(benchmark.get('results', [])) for benchmark in self.collected_data)
        
        summary = {
            "total_benchmarks": len(self.collected_data),
            "total_results": total_results,
            "collection_date": datetime.now().isoformat(),
            "data_source": "Hugging Face ASR Leaderboard"
        }
        
        with open(f'leaderboard_summary_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # Дополнительно сохраняем в CSV для удобства
        self._save_to_csv(timestamp)
        
        logging.info(f"Собрано {len(self.collected_data)} бенчмарков с {total_results} результатами")
        logging.info(f"Данные сохранены в {output_file}")
    
    def _save_to_csv(self, timestamp: str):
        """Сохранение данных в CSV формат"""
        if not self.collected_data:
            return
        
        # Создаем плоскую структуру для CSV
        csv_data = []
        for benchmark in self.collected_data:
            for result in benchmark.get('results', []):
                row = {
                    'benchmark_name': benchmark['benchmark_name'],
                    'model_name': result['model_name'],
                    'rank': result['rank'],
                    'code_link': result['code_link'],
                    'submission_date': result['submission_date']
                }
                
                # Добавляем метрики
                for metric in result['metrics']:
                    metric_key = f"{metric['type']}_{metric['dataset_split']}"
                    row[metric_key] = metric['value']
                
                csv_data.append(row)
        
        df = pd.DataFrame(csv_data)
        csv_filename = f"huggingface_leaderboard_{timestamp}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8')
        logging.info(f"Данные также сохранены в {csv_filename}")

def main():
    """Основная функция"""
    scraper = HuggingFaceLeaderboardScraper()
    data = scraper.collect_data()
    
    if data:
        total_results = sum(len(benchmark.get('results', [])) for benchmark in data)
        logging.info(f"Успешно собраны данные для {len(data)} бенчмарков с {total_results} результатами")
        print(f"Успешно собраны данные для {len(data)} бенчмарков с {total_results} результатами")
    else:
        logging.error("Не удалось собрать данные")
        print("Не удалось собрать данные")

if __name__ == "__main__":
    main()