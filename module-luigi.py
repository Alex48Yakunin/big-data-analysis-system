import luigi
import requests
import os
import tempfile
import pandas as pd
import shutil
from pathlib import Path
import zipfile
import tarfile
import zlib
import gzip
import io
import luigi
import pandas as pd
import os
from luigi import Task, LocalTarget


# Определяем путь к папке data в корне проекта
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = Path(PROJECT_ROOT) / 'data'

# Создаем директорию data, если ее еще нет
DATA_DIR.mkdir(parents=True, exist_ok=True)

class DownloadArchive(luigi.Task):
    dataset_id = luigi.Parameter()

    def output(self):
        return DATA_DIR / f"{self.dataset_id}_archive.tar"

    def complete(self):
        return self.output().exists()

    def run(self):
        url = f"https://www.ncbi.nlm.nih.gov/geo/download/?acc={self.dataset_id}&format=file&tool=luigi_task"
        response = requests.get(url)
        with self.output().open('wb') as f:
            f.write(response.content)

class ExtractArchive(luigi.Task):
    dataset_id = luigi.Parameter()

    def requires(self):
        return DownloadArchive(self.dataset_id)

    def output(self):
        return Path("data") / f"{self.dataset_id}_extracted"

    def complete(self):
        return self.output().exists()

    def run(self):
        input_path = str(self.input())
        output_path = self.output()

        # Создаем временную директорию
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Попытка распаковать архив
                if input_path.lower().endswith('.tar.gz'):
                    with gzip.open(input_path, 'rb') as f_in:
                        with open(os.path.join(temp_dir, 'temp.tar'), 'wb') as f_out:
                            f_out.write(zlib.decompress(f_in.read()))
                    
                    with tarfile.open(os.path.join(temp_dir, 'temp.tar'), 'r') as tar:
                        tar.extractall(path=temp_dir)
                
                elif input_path.lower().endswith('.gz'):
                    with gzip.open(input_path, 'rb') as f:
                        data = f.read()
                    with open(os.path.join(temp_dir, 'temp.txt'), 'wb') as f:
                        f.write(data)
                
                elif input_path.lower().endswith('.zip'):
                    with zipfile.ZipFile(input_path, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                
                else:
                    # Попытка распаковать обычный .tar
                    with tarfile.open(input_path, 'r') as tar:
                        tar.extractall(path=temp_dir)

                # Находим все вложенные архивы
                nested_archives = list(Path(temp_dir).glob('**/*.tar.gz')) + \
                                  list(Path(temp_dir).glob('**/*.gz')) + \
                                  list(Path(temp_dir).glob('**/*.zip'))

                # Распаковываем каждый вложенный архив
                for archive in nested_archives:
                    if archive.name.lower().endswith('.tar.gz'):
                        with gzip.open(archive, 'rb') as f_in:
                            with open(os.path.join(temp_dir, 'temp.tar'), 'wb') as f_out:
                                f_out.write(zlib.decompress(f_in.read()))
                        
                        with tarfile.open(os.path.join(temp_dir, 'temp.tar'), 'r') as tar:
                            tar.extractall(path=temp_dir)
                    elif archive.name.lower().endswith('.gz'):
                        with gzip.open(archive, 'rb') as f:
                            data = f.read()
                        with open(os.path.join(temp_dir, f"{archive.name}.txt"), 'wb') as f:
                            f.write(data)
                    elif archive.name.lower().endswith('.zip'):
                        with zipfile.ZipFile(archive, 'r') as zip_ref:
                            zip_ref.extractall(temp_dir)

                # Создаем директорию для результата
                result_dir = output_path.parent / f"{self.dataset_id}_extracted"
                os.makedirs(result_dir, exist_ok=True)

                # Переносим содержимое распакованных архивов в директорию результата
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        rel_path = os.path.relpath(root, temp_dir)
                        dest_path = result_dir / rel_path
                        dest_path.mkdir(parents=True, exist_ok=True)
                        shutil.move(os.path.join(root, file), dest_path)

                # Удаляем распакованные архивы
                for archive in nested_archives:
                    os.remove(archive)

            except Exception as e:
                print(f"Ошибка при распаковке архива: {e}")
                raise

class ProcessData(Task):
    dataset_id = luigi.Parameter()
   
    def requires(self):
        return ExtractArchive(self.dataset_id)

    def output(self):
        return LocalTarget(os.path.abspath(f"data/{self.dataset_id}_processed"))

    def run(self):
        def txt_to_dfs(file):
            dfs = {}
            with open(f"data/{file}") as f:
                write_key = None
                fio = io.StringIO()
                for line in f:
                    if line.startswith('['):
                        if write_key:
                            fio.seek(0)
                            header = None if write_key == 'Heading' else 'infer'
                            dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                            fio = io.StringIO()
                        write_key = line.strip('[]\n')
                        continue
                    if write_key:
                        fio.write(line)
                
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')

            # Сохраняем каждый DataFrame в отдельный TSV файл
            for key, df in dfs.items():
                filename = f"{key}.tsv"
                df.to_csv(f'./data/data-frames/{filename}', sep='\t', index=False, header=True)
                print(f"Сохранено: {filename}")
                
        txt_to_dfs('GSE68849_extracted/GPL10558_HumanHT-12_V4_0_R1_15002873_B.txt.gz.txt')
        txt_to_dfs('GSE68849_extracted/GPL10558_HumanHT-12_V4_0_R2_15002873_B.txt.gz.txt')
        
        tables = pd.read_csv('data/data-frames/Probes.tsv', sep='\t')

        # Удаляем нежелательные колонки
        columns_to_remove = [
            'Definition', 
            'Ontology_Component', 
            'Ontology_Process', 
            'Ontology_Function', 
            'Synonyms', 
            'Obsolete_Probe_Id', 
            'Probe_Sequence'
            ]
        tables = tables.drop(columns=columns_to_remove)
        
        # Сохраняем обновленную таблицу Probes в отдельный файл
        probes_output_path = os.path.join(os.path.dirname('data/data-frames/'), 'Probes_updated.tsv')
        tables.to_csv(probes_output_path, index=False, sep='\t')

def main():
    dataset_id = "GSE68849"
    
    # Создаем экземпляр Task
    task = DownloadArchive(dataset_id)
    
    # Добавляем дополнительные задачи в цепочку
    task = ExtractArchive(dataset_id)
    task = ProcessData(dataset_id)
    
    # Запускаем пайплайн
    luigi.build([task], local_scheduler=True)

if __name__ == '__main__':
    main()
