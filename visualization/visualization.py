#!/usr/bin/env python3
"""
Скрипты для визуализации данных ASR/TTS систем
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as pyo
from analysis.data_analysis import DataAnalyzer
import logging

# Настройка стилей
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

# Настройка логирования
logging.basicConfig(level=logging.INFO)

class DataVisualizer:
    def __init__(self):
        self.analyzer = DataAnalyzer()
        self.results = None
    
    def load_analysis_results(self):
        """
        Загружает результаты анализа
        """
        if self.results is None:
            self.results = self.analyzer.run_full_analysis()
    
    def plot_wer_vs_year(self, save_path="wer_vs_year.png"):
        """
        График зависимости WER от года публикации модели для ASR
        """
        self.load_analysis_results()
        wer_data = self.results['wer_vs_year']
        
        if not wer_data:
            logging.warning("Нет данных WER для визуализации")
            return
        
        df = pd.DataFrame(wer_data)
        
        # Создаем график
        plt.figure(figsize=(12, 8))
        
        # Разные цвета для разных датасетов
        datasets = df['dataset'].unique()
        colors = plt.cm.Set3(np.linspace(0, 1, len(datasets)))
        
        for i, dataset in enumerate(datasets):
            dataset_data = df[df['dataset'] == dataset]
            plt.scatter(dataset_data['year'], dataset_data['wer'], 
                       label=dataset, alpha=0.7, s=60, color=colors[i])
        
        # Линия тренда
        if len(df) > 1:
            z = np.polyfit(df['year'], df['wer'], 1)
            p = np.poly1d(z)
            plt.plot(df['year'], p(df['year']), "r--", alpha=0.8, linewidth=2)
        
        plt.xlabel('Год публикации', fontsize=12)
        plt.ylabel('WER (%)', fontsize=12)
        plt.title('Зависимость WER от года публикации модели (ASR)', fontsize=14, fontweight='bold')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"График WER vs Year сохранен: {save_path}")
    
    def plot_mos_vs_year(self, save_path="mos_vs_year.png"):
        """
        График зависимости MOS от года публикации модели для TTS
        """
        self.load_analysis_results()
        mos_data = self.results['mos_vs_year']
        
        if not mos_data:
            logging.warning("Нет данных MOS для визуализации")
            return
        
        df = pd.DataFrame(mos_data)
        
        plt.figure(figsize=(12, 8))
        
        # Разные цвета для разных датасетов
        datasets = df['dataset'].unique()
        colors = plt.cm.Set2(np.linspace(0, 1, len(datasets)))
        
        for i, dataset in enumerate(datasets):
            dataset_data = df[df['dataset'] == dataset]
            plt.scatter(dataset_data['year'], dataset_data['mos'], 
                       label=dataset, alpha=0.7, s=60, color=colors[i])
        
        # Линия тренда
        if len(df) > 1:
            z = np.polyfit(df['year'], df['mos'], 1)
            p = np.poly1d(z)
            plt.plot(df['year'], p(df['year']), "r--", alpha=0.8, linewidth=2)
        
        plt.xlabel('Год публикации', fontsize=12)
        plt.ylabel('MOS', fontsize=12)
        plt.title('Зависимость MOS от года публикации модели (TTS)', fontsize=14, fontweight='bold')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"График MOS vs Year сохранен: {save_path}")
    
    def plot_architecture_distribution(self, save_path="architecture_distribution.png"):
        """
        График распределения архитектур
        """
        self.load_analysis_results()
        arch_data = self.results['architecture_distribution']
        
        if not arch_data:
            logging.warning("Нет данных об архитектурах для визуализации")
            return
        
        df = pd.DataFrame(arch_data)
        
        plt.figure(figsize=(12, 8))
        
        # Сортируем по количеству
        df = df.sort_values('count', ascending=True)
        
        bars = plt.barh(df['architecture'], df['count'])
        
        # Добавляем значения на столбцы
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                    f'{int(width)}', ha='left', va='center')
        
        plt.xlabel('Количество систем', fontsize=12)
        plt.ylabel('Архитектура', fontsize=12)
        plt.title('Распределение архитектур ASR/TTS систем', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3, axis='x')
        plt.tight_layout()
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"График распределения архитектур сохранен: {save_path}")
    
    def plot_top_developers(self, save_path="top_developers.png"):
        """
        График топ разработчиков
        """
        self.load_analysis_results()
        dev_data = self.results['top_developers'][:10]  # Топ-10
        
        if not dev_data:
            logging.warning("Нет данных о разработчиках для визуализации")
            return
        
        df = pd.DataFrame(dev_data)
        
        plt.figure(figsize=(12, 8))
        
        bars = plt.bar(range(len(df)), df['system_count'])
        
        # Добавляем значения на столбцы
        for i, bar in enumerate(bars):
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, height + 0.1, 
                    f'{int(height)}', ha='center', va='bottom')
        
        plt.xlabel('Разработчики', fontsize=12)
        plt.ylabel('Количество систем', fontsize=12)
        plt.title('Топ-10 разработчиков по количеству систем', fontsize=14, fontweight='bold')
        plt.xticks(range(len(df)), df['developer'], rotation=45, ha='right')
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"График топ разработчиков сохранен: {save_path}")
    
    def plot_yearly_trends(self, save_path="yearly_trends.png"):
        """
        График трендов по годам
        """
        self.load_analysis_results()
        trends_data = self.results['yearly_trends']
        
        if not trends_data:
            logging.warning("Нет данных о трендах для визуализации")
            return
        
        df = pd.DataFrame(trends_data)
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # График количества систем по годам
        ax1.plot(df['year'], df['systems_count'], marker='o', linewidth=2, markersize=6)
        ax1.set_xlabel('Год', fontsize=12)
        ax1.set_ylabel('Количество систем', fontsize=12)
        ax1.set_title('Количество новых систем по годам', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        # График средних скачиваний по годам
        ax2.plot(df['year'], df['avg_downloads'], marker='s', linewidth=2, markersize=6, color='orange')
        ax2.set_xlabel('Год', fontsize=12)
        ax2.set_ylabel('Средние скачивания', fontsize=12)
        ax2.set_title('Средние скачивания по годам', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        logging.info(f"График трендов по годам сохранен: {save_path}")
    
    def create_interactive_wer_plot(self, save_path="interactive_wer.html"):
        """
        Интерактивный график WER с использованием Plotly
        """
        self.load_analysis_results()
        wer_data = self.results['wer_vs_year']
        
        if not wer_data:
            logging.warning("Нет данных WER для интерактивной визуализации")
            return
        
        df = pd.DataFrame(wer_data)
        
        fig = px.scatter(df, x='year', y='wer', color='dataset',
                        hover_data=['model_name', 'architecture'],
                        title='Интерактивный график: WER vs Год публикации (ASR)',
                        labels={'year': 'Год публикации', 'wer': 'WER (%)'})
        
        # Добавляем линию тренда
        if len(df) > 1:
            z = np.polyfit(df['year'], df['wer'], 1)
            p = np.poly1d(z)
            x_trend = np.linspace(df['year'].min(), df['year'].max(), 100)
            y_trend = p(x_trend)
            
            fig.add_trace(go.Scatter(x=x_trend, y=y_trend,
                                   mode='lines',
                                   name='Тренд',
                                   line=dict(dash='dash', color='red')))
        
        fig.update_layout(
            width=1000,
            height=600,
            showlegend=True
        )
        
        pyo.plot(fig, filename=save_path, auto_open=False)
        logging.info(f"Интерактивный график WER сохранен: {save_path}")
    
    def create_benchmark_comparison(self, save_path="benchmark_comparison.png"):
        """
        Сравнение результатов бенчмарков
        """
        self.load_analysis_results()
        benchmark_data = self.results['benchmark_analysis']
        
        if not benchmark_data:
            logging.warning("Нет данных бенчмарков для визуализации")
            return
        
        df = pd.DataFrame(benchmark_data)
        
        # Группируем по бенчмаркам и метрикам
        benchmark_metrics = df.groupby(['benchmark_name', 'metric_type'])['value'].apply(list).reset_index()
        
        plt.figure(figsize=(15, 10))
        
        # Создаем подграфики для каждого бенчмарка
        unique_benchmarks = df['benchmark_name'].unique()
        n_benchmarks = len(unique_benchmarks)
        
        if n_benchmarks > 0:
            fig, axes = plt.subplots(n_benchmarks, 1, figsize=(15, 5*n_benchmarks))
            if n_benchmarks == 1:
                axes = [axes]
            
            for i, benchmark in enumerate(unique_benchmarks):
                benchmark_df = df[df['benchmark_name'] == benchmark]
                
                # Сортируем по рангу
                benchmark_df = benchmark_df.sort_values('rank')
                
                # Создаем график для каждого бенчмарка
                for metric_type in benchmark_df['metric_type'].unique():
                    metric_data = benchmark_df[benchmark_df['metric_type'] == metric_type]
                    axes[i].plot(metric_data['rank'], metric_data['value'], 
                               marker='o', label=f'{metric_type}', linewidth=2)
                
                axes[i].set_xlabel('Ранг', fontsize=12)
                axes[i].set_ylabel('Значение метрики', fontsize=12)
                axes[i].set_title(f'{benchmark}', fontsize=14, fontweight='bold')
                axes[i].legend()
                axes[i].grid(True, alpha=0.3)
                axes[i].invert_xaxis()  # Лучшие результаты слева
            
            plt.tight_layout()
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            plt.show()
            
            logging.info(f"График сравнения бенчмарков сохранен: {save_path}")
    
    def create_all_visualizations(self):
        """
        Создает все визуализации
        """
        logging.info("Начинаем создание всех визуализаций")
        
        try:
            self.plot_wer_vs_year()
            self.plot_mos_vs_year()
            self.plot_architecture_distribution()
            self.plot_top_developers()
            self.plot_yearly_trends()
            self.create_interactive_wer_plot()
            self.create_benchmark_comparison()
            
            logging.info("Все визуализации созданы успешно")
            
        except Exception as e:
            logging.error(f"Ошибка при создании визуализаций: {e}")

def main():
    """
    Основная функция для создания визуализаций
    """
    visualizer = DataVisualizer()
    visualizer.create_all_visualizations()

if __name__ == "__main__":
    main()
