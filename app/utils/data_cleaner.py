import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

class DataCleaner:
    def __init__(self, stop_words_lang='spanish'):
        nltk.download('stopwords')
        nltk.download('punkt')
        nltk.download('punkt_tab')
        self.stop_words = set(stopwords.words(stop_words_lang))
    
    def load_data(self, file_path, file_type='csv', sep=';'):
        if file_type == 'csv':
            dataframe = pd.read_csv(file_path, sep=sep, encoding='utf-8')
        elif file_type == 'parquet':
            dataframe = pd.read_parquet(file_path)
        else:
            raise ValueError("Unsupported file type. Use 'csv' or 'parquet'.")
        return dataframe

    def cast_columns(self, dataframe, columns_to_cast):
        dataframe = dataframe.astype({col: 'string' for col in columns_to_cast})
        return dataframe

    def encode_categorical_columns(self, dataframe, categorical_columns):
        for col in categorical_columns:
            dataframe[col] = dataframe[col].str.lower().map({
                'si': 1, 'no': 0, 'no definido': -1, 'válido': 1, 'no válido': 0, 'no d': -1
            }).fillna(dataframe[col])
            dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
        return dataframe
    
    def remove_extra_punct(self, text):
        text = text.lower()
        text = re.sub(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)', "", text)
        text = re.sub(r'[\!\\"\#\$\%\&\'\(\)\*\+\,\-\.\/\:\;\<\=\>\?\@\[\\\]\^_\`\{\|\}\~]', "", text)
        text = re.sub(r'\d+', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        tokens = word_tokenize(text)
        tokens = [w for w in tokens if not w.lower() in self.stop_words]
        return " ".join(tokens)
    
    def clean_text_columns(self, dataframe, text_columns):
        for col in text_columns:
            dataframe[col] = dataframe[col].fillna('').str.lower().apply(self.remove_extra_punct)
        return dataframe

    def clean_numeric_columns(self, dataframe, numeric_columns):
        values_to_replace = ['', 'unspecified', 'nodefinido', 'sin descripcion']
        for col in numeric_columns:
            dataframe[col] = dataframe[col].replace(values_to_replace, '0')
        dataframe[numeric_columns] = dataframe[numeric_columns].apply(pd.to_numeric, errors='coerce').fillna(0)
        return dataframe

    def clean_date_columns(self, dataframe, date_columns):
        for col in date_columns:
            dataframe[col] = dataframe[col].fillna('1900-01-01').str.replace('T00:00:00.000', '', regex=True)
            dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')
        return dataframe

    def clean_url_columns(self, dataframe, url_columns):
        for col in url_columns:
            dataframe[col] = dataframe[col].replace(
                [r"\{ 'url ' : '", r" \? numconstancia= ' \}", r'\{ "url " : "', r' \? numconstancia= " \}', r"\{'", r"'\}", r'\{"', r'"\}', 
                 r"\{'url': '", r"'\}", r'\{"url": "', r'"\}'], '', regex=True)
        return dataframe

    def clean_descriptions_columns(self, dataframe, cols):
        for col in cols:
            dataframe[col] = dataframe[col].replace(r'[\x00-\x1F\x7F-\x9F]', '', regex=True)
        return dataframe

    def clean_printable_chars(self, series):
        return series.apply(lambda x: ''.join([char for char in str(x) if char.isprintable()]))

    def save_data(self, dataframe, output_path, file_type='parquet'):
        if file_type == 'parquet':
            dataframe.to_parquet(output_path, index=False)
        elif file_type == 'csv':
            dataframe.to_csv(output_path, sep=';', index=False, encoding='utf-8')
        else:
            raise ValueError("Unsupported file type. Use 'parquet' or 'csv'.")
    