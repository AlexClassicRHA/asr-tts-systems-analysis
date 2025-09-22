# План сбора данных для ASR/TTS систем

## Группа 1: Модели с Hugging Face

### Источники данных:
- https://huggingface.co/models?pipeline_tag=audio-to-audio
- https://huggingface.co/models?pipeline_tag=automatic-speech-recognition  
- https://huggingface.co/models?pipeline_tag=text-to-speech

### Данные для сбора:
- **Название модели** → `systems.название`
- **Организация-автор** → `systems.разработчик`
- **Задачи** → `systems.описание` + связь с `functional_purposes`
- **Архитектура** → `systems.архитектура`
- **Количество скачиваний** → `systems.количество_скачиваний`
- **Ссылки на бумаги** → `system_papers`
- **Поддерживаемые языки** → `systems.поддерживаемые_языки`
- **Лицензия** → `systems.тип_лицензии`
- **Дата создания** → `systems.год_первого_релиза`

### Структура данных для группы 1:
```json
{
  "model_name": "string",
  "author_organization": "string", 
  "tasks": ["array of strings"],
  "architecture": "string",
  "downloads": "number",
  "papers": ["array of paper objects"],
  "languages": ["array of strings"],
  "license": "string",
  "created_date": "string"
}
```

## Группа 2: Датасеты с Hugging Face и OpenSLR

### Источники данных:
- https://huggingface.co/datasets
- https://openslr.org/

### Данные для сбора:
- **Название датасета** → `datasets.название`
- **Описание** → `datasets.описание`
- **Объем (часы/гигабайты)** → `datasets.объем_часы`, `datasets.объем_гигабайты`
- **Язык** → `datasets.язык`
- **Лицензия** → `datasets.лицензия`
- **Источник** → `datasets.источник` ('huggingface' или 'openslr')
- **Ссылка** → `datasets.ссылка`

### Структура данных для группы 2:
```json
{
  "dataset_name": "string",
  "description": "string",
  "size_hours": "number",
  "size_gb": "number", 
  "language": "string",
  "license": "string",
  "source": "string",
  "url": "string"
}
```

## Группа 3: Научные статьи

### Источники данных:
- Google Research
- arXiv.org
- Yandex Research
- Поиск по ключевым словам: "speech recognition", "text to speech", "speech synthesis", "voice cloning"

### Данные для сбора:
- **Название модели** → `systems.название`
- **Метрики (WER на LibriSpeech, MOS для TTS)** → `system_metrics`
- **Ссылка на arXiv** → `system_papers.ссылка_arxiv`
- **Год публикации** → `system_papers.год_публикации`
- **Авторы** → `system_papers.авторы`
- **Название статьи** → `system_papers.название_статьи`

### Структура данных для группы 3:
```json
{
  "model_name": "string",
  "paper_title": "string",
  "arxiv_link": "string",
  "publication_year": "number",
  "authors": ["array of strings"],
  "metrics": [
    {
      "type": "WER|MOS|BLEU|etc",
      "value": "number",
      "dataset": "string",
      "language": "string"
    }
  ]
}
```

## Группа 4: Бенчмарки и лидерборды

### Источники данных:
- https://paperswithcode.com/
- https://superbbenchmark.org/
- https://leaderboard.allenai.org/
- https://paperswithcode.com/sota (State of the Art)
- Специализированные бенчмарки: LibriSpeech, Common Voice, VoxForge

### Данные для сбора:
- **Название бенчмарка** → `benchmarks.название`
- **Задачи** → `benchmarks.задачи`
- **Лучшие модели** → `benchmark_results`
- **Результаты** → `benchmark_results.результаты`
- **Датасет** → `benchmarks.датасет`
- **Метрики** → `benchmark_results.метрики`
- **Ссылка на бенчмарк** → `benchmarks.ссылка`

### Структура данных для группы 4:
```json
{
  "benchmark_name": "string",
  "tasks": ["array of strings"],
  "dataset": "string",
  "url": "string",
  "description": "string",
  "results": [
    {
      "model_name": "string",
      "rank": "number",
      "metrics": [
        {
          "type": "WER|MOS|BLEU|etc",
          "value": "number",
          "dataset_split": "test|dev|validation"
        }
      ],
      "paper_link": "string",
      "code_link": "string",
      "submission_date": "string"
    }
  ]
}
```

## Общие рекомендации по сбору данных:

### Для всех групп:
1. **Качество данных**: Проверять актуальность и достоверность информации
2. **Стандартизация**: Использовать единые форматы для языков, лицензий, метрик
3. **Валидация**: Проверять корректность ссылок и доступность ресурсов
4. **Документирование**: Записывать источник каждого собранного элемента

### Форматы файлов для сбора:
- **JSON** для структурированных данных
- **CSV** для табличных данных
- **Markdown** для описаний и заметок

### Организация файлов:
```
data_collection/
├── group1_huggingface_models/
│   ├── models_data.json
│   ├── collection_log.md
│   └── raw_data/
├── group2_datasets/
│   ├── datasets_data.json
│   ├── collection_log.md
│   └── raw_data/
├── group3_papers/
│   ├── papers_data.json
│   ├── collection_log.md
│   └── raw_data/
└── group4_benchmarks/
    ├── benchmarks_data.json
    ├── collection_log.md
    └── raw_data/
```
