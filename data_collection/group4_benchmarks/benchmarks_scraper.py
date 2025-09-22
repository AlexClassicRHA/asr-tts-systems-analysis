import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time


def parse_huggingface_table():
    # Настройка драйвера
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        # Открытие страницы
        print("Открытие страницы...")
        driver.get("https://huggingface.co/spaces/hf-audio/open_asr_leaderboard")
        
        # Ожидание загрузки страницы
        time.sleep(5)
        
        # Поиск и переключение на iframe
        print("Поиск iframe...")
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "iframe"))
        )
        
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        print(f"Найдено iframe: {len(iframes)}")
        
        # Переключение на основной iframe с контентом
        driver.switch_to.frame(iframes[0])
        print("Переключились на iframe")
        
        # Ожидание загрузки таблицы внутри iframe
        print("Ожидание загрузки таблицы...")
        
        # Ждем появления таблицы
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "svelte-1xyl3gk"))
        )
        print("Таблица найдена")
        
        # Дадим дополнительное время для загрузки данных
        time.sleep(5)
        
        # Извлечение заголовков таблицы
        headers = []
        try:
            # Попробуем найти заголовки через XPath
            header_elements = driver.find_elements(By.XPATH, "//table//thead//th")
            
            for header in header_elements:
                header_text = header.text.strip()
                if header_text:  # Пропускаем пустые заголовки
                    headers.append(header_text)
            
            print(f"Найдено заголовков: {len(headers)}")
            print("Заголовки:", headers)
        except Exception as e:
            print(f"Ошибка при извлечении заголовков: {e}")
            # Используем заранее известные заголовки
            headers = ["model", "Average WER ⬇️", "RTFx ⬆️️", "License", "AMI", "Earnings22", 
                      "Gigaspeech", "LS Clean", "LS Other", "SPGISpeech", "Tedlium", "Voxpopuli"]
        
        # Находим элемент таблицы с прокруткой
        table_container = driver.find_element(By.CLASS_NAME, "svelte-1xyl3gk")
        
        # Функция для медленной пошаговой прокрутки таблицы без дубликатов
        def extract_data_without_duplicates():
            data = []
            seen_models = set()  # Для отслеживания уже обработанных моделей
            scroll_step = 100  # Малый шаг прокрутки
            max_scrolls = 100  # Максимальное количество прокруток
            scroll_count = 0
            no_new_models_count = 0  # Счетчик прокруток без новых моделей
            
            while scroll_count < max_scrolls and no_new_models_count < 5:
                # Извлекаем текущие строки
                rows = driver.find_elements(By.XPATH, "//table//tbody//tr")
                current_row_count = len(rows)
                print(f"Найдено строк: {current_row_count}")
                
                # Извлекаем данные из текущих строк
                new_models_found = 0
                for i, row in enumerate(rows):
                    try:
                        cols = row.find_elements(By.TAG_NAME, "td")
                        if cols and len(cols) == len(headers):
                            row_data = [col.text.strip() for col in cols]
                            model_name = row_data[0]  # Первый столбец - название модели
                            
                            # Проверяем, что строка не пустая и модель еще не встречалась
                            if model_name and model_name not in seen_models:
                                data.append(row_data)
                                seen_models.add(model_name)
                                new_models_found += 1
                                print(f"Добавлена модель {len(data)}: {model_name}")
                    except Exception as e:
                        print(f"Ошибка при обработке строки: {e}")
                
                # Если не нашли новых моделей, увеличиваем счетчик
                if new_models_found == 0:
                    no_new_models_count += 1
                    print(f"Новых моделей не найдено. Попытка {no_new_models_count}/5")
                else:
                    no_new_models_count = 0  # Сбрасываем счетчик, если нашли новые модели
                
                # Если мы собрали достаточно данных, выходим
                if len(data) >= 60:  # Ожидаем около 60 строк
                    print(f"Собрано достаточно данных ({len(data)} моделей), завершаем прокрутку")
                    break
                
                # Медленно прокручиваем таблицу вниз на небольшой шаг
                current_scroll_position = driver.execute_script("return arguments[0].scrollTop", table_container)
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollTop + arguments[1]", 
                                     table_container, scroll_step)
                time.sleep(0.5)  # Небольшая пауза между прокрутками
                
                # Проверяем, достигли ли мы конца таблицы
                new_scroll_position = driver.execute_script("return arguments[0].scrollTop", table_container)
                if new_scroll_position == current_scroll_position:
                    print("Достигнут конец таблицы")
                    break
                
                scroll_count += 1
                print(f"Прокрутка {scroll_count}/{max_scrolls}, позиция: {new_scroll_position}, новых моделей: {new_models_found}")
            
            return data
        
        # Извлечение данных без дубликатов
        data = extract_data_without_duplicates()
        
        # Создание DataFrame
        if headers and data:
            df = pd.DataFrame(data, columns=headers)
            
            # Сохранение в CSV
            df.to_csv("huggingface_leaderboard.csv", index=False, encoding='utf-8')
            print(f"Данные сохранены в CSV. Найдено уникальных моделей: {len(data)}")
            
            return df
        else:
            print("Не удалось извлечь данные или заголовки")
            return None
        
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
        
        # Сохранение скриншота и HTML для отладки
        driver.save_screenshot("error_screenshot.png")
        with open("page_source.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        return None
        
    finally:
        # Возврат к основному контенту и закрытие драйвера
        try:
            driver.switch_to.default_content()
        except:
            pass
        driver.quit()

# Запуск парсинга
if __name__ == "__main__":
    df = parse_huggingface_table()
    if df is not None and not df.empty:
        print("Успешно извлечены данные:")
        print(f"Всего уникальных моделей: {len(df)}")
    else:
        print("Не удалось извлечь данные")