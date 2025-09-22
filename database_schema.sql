-- SQL схема для ASR/TTS систем
-- Создание базы данных

CREATE DATABASE IF NOT EXISTS asr_tts_systems;
USE asr_tts_systems;

-- Таблица типов словарей
CREATE TABLE vocabulary_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    тип VARCHAR(50) NOT NULL UNIQUE,
    описание TEXT,
    диапазон_слов VARCHAR(100)
);

-- Таблица функциональных назначений
CREATE TABLE functional_purposes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    назначение VARCHAR(100) NOT NULL UNIQUE,
    описание TEXT
);

-- Основная таблица систем
CREATE TABLE systems (
    id INT AUTO_INCREMENT PRIMARY KEY,
    название VARCHAR(255) NOT NULL,
    разработчик VARCHAR(255),
    год_первого_релиза INT,
    описание TEXT,
    ссылка_на_источник VARCHAR(500),
    тип_лицензии VARCHAR(100),
    архитектура VARCHAR(100),
    поддерживаемые_языки TEXT,
    количество_скачиваний BIGINT DEFAULT 0,
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    дата_обновления TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Таблица связи систем и типов словарей (многие-ко-многим)
CREATE TABLE system_vocabulary_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    system_id INT,
    vocabulary_type_id INT,
    FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE,
    FOREIGN KEY (vocabulary_type_id) REFERENCES vocabulary_types(id) ON DELETE CASCADE,
    UNIQUE KEY unique_system_vocabulary (system_id, vocabulary_type_id)
);

-- Таблица связи систем и функциональных назначений (многие-ко-многим)
CREATE TABLE system_functional_purposes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    system_id INT,
    functional_purpose_id INT,
    FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE,
    FOREIGN KEY (functional_purpose_id) REFERENCES functional_purposes(id) ON DELETE CASCADE,
    UNIQUE KEY unique_system_purpose (system_id, functional_purpose_id)
);

-- Таблица метрик систем
CREATE TABLE system_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    system_id INT,
    метрика_тип VARCHAR(50) NOT NULL,
    значение DECIMAL(10,4),
    датасет VARCHAR(255),
    язык VARCHAR(50),
    дата_измерения DATE,
    FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE
);

-- Таблица научных статей
CREATE TABLE system_papers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    system_id INT,
    название_статьи VARCHAR(500) NOT NULL,
    ссылка_arxiv VARCHAR(500),
    год_публикации INT,
    авторы TEXT,
    FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE
);

-- Таблица датасетов
CREATE TABLE datasets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    название VARCHAR(255) NOT NULL,
    описание TEXT,
    объем_часы DECIMAL(10,2),
    объем_гигабайты DECIMAL(10,2),
    язык VARCHAR(100),
    лицензия VARCHAR(100),
    источник VARCHAR(100), -- 'huggingface' или 'openslr'
    ссылка VARCHAR(500),
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица бенчмарков
CREATE TABLE benchmarks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    название VARCHAR(255) NOT NULL,
    задачи TEXT,
    датасет VARCHAR(255),
    описание TEXT,
    ссылка VARCHAR(500),
    источник VARCHAR(100), -- 'paperswithcode', 'superbbenchmark', 'allenai', etc.
    дата_создания TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица результатов бенчмарков
CREATE TABLE benchmark_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    benchmark_id INT,
    system_id INT,
    ранг INT,
    метрика_тип VARCHAR(50) NOT NULL,
    значение DECIMAL(10,4),
    датасет_раздел VARCHAR(50), -- 'test', 'dev', 'validation'
    ссылка_на_статью VARCHAR(500),
    ссылка_на_код VARCHAR(500),
    дата_отправки DATE,
    FOREIGN KEY (benchmark_id) REFERENCES benchmarks(id) ON DELETE CASCADE,
    FOREIGN KEY (system_id) REFERENCES systems(id) ON DELETE CASCADE
);

-- Вставка базовых данных для типов словарей
INSERT INTO vocabulary_types (тип, описание, диапазон_слов) VALUES
('малый', 'Системы с ограниченным словарем', 'до 1000 слов'),
('средний', 'Системы со средним словарем', '1000-10000 слов'),
('большой (LVCSR)', 'Large Vocabulary Continuous Speech Recognition', '10000+ слов');

-- Вставка базовых данных для функциональных назначений
INSERT INTO functional_purposes (назначение, описание) VALUES
('командная', 'Распознавание голосовых команд'),
('диктовка', 'Преобразование речи в текст для диктовки'),
('понимание (SLU)', 'Spoken Language Understanding - понимание смысла речи'),
('диалоговая', 'Диалоговые системы и чат-боты');

-- Создание индексов для оптимизации запросов
CREATE INDEX idx_systems_developer ON systems(разработчик);
CREATE INDEX idx_systems_year ON systems(год_первого_релиза);
CREATE INDEX idx_systems_license ON systems(тип_лицензии);
CREATE INDEX idx_metrics_type ON system_metrics(метрика_тип);
CREATE INDEX idx_metrics_dataset ON system_metrics(датасет);
CREATE INDEX idx_papers_year ON system_papers(год_публикации);
CREATE INDEX idx_benchmarks_source ON benchmarks(источник);
CREATE INDEX idx_benchmark_results_rank ON benchmark_results(ранг);
CREATE INDEX idx_benchmark_results_metric ON benchmark_results(метрика_тип);
