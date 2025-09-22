# ERD Модель для ASR/TTS Систем

## Описание сущностей

### 1. systems (Системы)
Основная сущность, описывающая ASR/TTS системы.

**Атрибуты:**
- `id` (INTEGER, PRIMARY KEY) - уникальный идентификатор
- `название` (VARCHAR) - название системы
- `разработчик` (VARCHAR) - организация или автор
- `год_первого_релиза` (INTEGER) - год первого релиза
- `описание` (TEXT) - подробное описание системы
- `ссылка_на_источник` (VARCHAR) - URL на официальный источник
- `тип_лицензии` (VARCHAR) - тип лицензии (MIT, Apache, Commercial, etc.)
- `архитектура` (VARCHAR) - архитектура модели (Transformer, RNN, CNN, etc.)
- `поддерживаемые_языки` (TEXT) - список поддерживаемых языков

### 2. vocabulary_types (Типы словарей)
Классификация систем по размеру словаря.

**Атрибуты:**
- `id` (INTEGER, PRIMARY KEY) - уникальный идентификатор
- `тип` (VARCHAR) - тип словаря: "малый", "средний", "большой (LVCSR)"
- `описание` (TEXT) - описание типа словаря
- `диапазон_слов` (VARCHAR) - примерный диапазон слов (например, "до 1000", "1000-10000", "10000+")

### 3. functional_purposes (Функциональные назначения)
Классификация систем по функциональному назначению.

**Атрибуты:**
- `id` (INTEGER, PRIMARY KEY) - уникальный идентификатор
- `назначение` (VARCHAR) - назначение: "командная", "диктовка", "понимание (SLU)", "диалоговая"
- `описание` (TEXT) - описание функционального назначения

## Связи

### Связь многие-ко-многим: systems ↔ vocabulary_types
- Таблица связи: `system_vocabulary_types`
- Атрибуты: `system_id`, `vocabulary_type_id`

### Связь многие-ко-многим: systems ↔ functional_purposes  
- Таблица связи: `system_functional_purposes`
- Атрибуты: `system_id`, `functional_purpose_id`

## Дополнительные таблицы для метрик

### 4. system_metrics (Метрики систем)
- `id` (INTEGER, PRIMARY KEY)
- `system_id` (INTEGER, FOREIGN KEY) - ссылка на systems
- `метрика_тип` (VARCHAR) - тип метрики (WER, MOS, BLEU, etc.)
- `значение` (DECIMAL) - значение метрики
- `датасет` (VARCHAR) - датасет для оценки (LibriSpeech, Common Voice, etc.)
- `язык` (VARCHAR) - язык для метрики

### 5. system_papers (Научные статьи)
- `id` (INTEGER, PRIMARY KEY)
- `system_id` (INTEGER, FOREIGN KEY) - ссылка на systems
- `название_статьи` (VARCHAR) - название статьи
- `ссылка_arxiv` (VARCHAR) - ссылка на arXiv
- `год_публикации` (INTEGER) - год публикации
- `авторы` (TEXT) - список авторов

### 6. benchmarks (Бенчмарки)
- `id` (INTEGER, PRIMARY KEY)
- `название` (VARCHAR) - название бенчмарка
- `задачи` (TEXT) - описание задач бенчмарка
- `датасет` (VARCHAR) - используемый датасет
- `описание` (TEXT) - описание бенчмарка
- `ссылка` (VARCHAR) - ссылка на бенчмарк
- `источник` (VARCHAR) - источник (paperswithcode, superbbenchmark, etc.)

### 7. benchmark_results (Результаты бенчмарков)
- `id` (INTEGER, PRIMARY KEY)
- `benchmark_id` (INTEGER, FOREIGN KEY) - ссылка на benchmarks
- `system_id` (INTEGER, FOREIGN KEY) - ссылка на systems
- `ранг` (INTEGER) - место в рейтинге
- `метрика_тип` (VARCHAR) - тип метрики (WER, MOS, BLEU)
- `значение` (DECIMAL) - значение метрики
- `датасет_раздел` (VARCHAR) - раздел датасета (test, dev, validation)
- `ссылка_на_статью` (VARCHAR) - ссылка на статью
- `ссылка_на_код` (VARCHAR) - ссылка на код
- `дата_отправки` (DATE) - дата отправки результата
