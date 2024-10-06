import polars as pd
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from functools import reduce
import pyspark.sql.functions as F

def load_data(s2_bronze, year=2024, id_contract_column='id_contrato'):
    s2_silver = pd.read_csv(f'../data/silver/S2/{year}/S2.csv', sep=';')
    # Se separan los datos que no han sido limpiados
    s2_to_clean = s2_bronze[~(s2_bronze[id_contract_column].isin(s2_silver[id_contract_column].to_list()))]
    return s2_silver, s2_to_clean

def cast_columns(dataframe, columns_to_cast):
    columns = [pd.col(col).cast(pd.Utf8) for col in columns_to_cast]
    dataframe = dataframe.with_columns(columns)
    return dataframe

def encode_categorical_columns(dataframe: pd.DataFrame, categorical_columns: list[str]) -> pd.DataFrame:
    # Crear una lista de expresiones para cada columna categórica
    expressions = []
    
    for col in categorical_columns:
        expressions.append(
            pd.when(pd.col(col).str.to_lowercase() == 'si').then(1)
              .when(pd.col(col).str.to_lowercase() == 'no').then(0)
              .when(pd.col(col).str.to_lowercase() == 'no definido').then(-1)
              .when(pd.col(col).str.to_lowercase() == 'válido').then(1)
              .when(pd.col(col).str.to_lowercase() == 'no válido').then(0)
              .when(pd.col(col).str.to_lowercase() == 'no d').then(-1)
              .otherwise(pd.col(col))
              .cast(pd.Int32)
              .alias(col)
        )

    # Aplicar todas las expresiones a la vez
    dataframe = dataframe.with_columns(*expressions)
    
    return dataframe

stop_words = set(stopwords.words("spanish"))

def remove_extra_punct(text):
    
    text = text.lower()
    text = re.sub(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)', "", text)
    text = re.sub(r'[\\!\\"\\#\\$\\%\\&\\\'\\(\\)\\*\\+\\,\\-\\.\\/\\:\\;\\<\\=\\>\\?\\@\\[\\\\\\]\\^_\\`\\{\\|\\}\\~]', "", text)
    text = re.sub(r'\#\.', '', text)
    text = re.sub(r'\n', ' ', text)
    text = re.sub(r'  ', ' ', text)
    text = re.sub(r'´', '',text)
    text = re.sub(r',', '',text)
    text = re.sub(r'\-', '', text)
    text = re.sub(r'á', 'a', text)
    text = re.sub(r'é', 'e', text)
    text = re.sub(r'í', 'i', text)
    text = re.sub(r'ó', 'o', text)
    text = re.sub(r'ú', 'u', text)
    text = re.sub(r'ò', 'o', text)
    text = re.sub(r'à', 'a', text)
    text = re.sub(r'è', 'e', text)
    text = re.sub(r'ì', 'i', text)
    text = re.sub(r'ù', 'u', text)
    text = re.sub("\d+", ' ', text)
    text = re.sub("\\s+", ' ', text)
    
    tokens = word_tokenize(text)
    tokens = [w for w in tokens if not w.lower() in stop_words]
    tokens = " ".join(tokens)
    
    return tokens

def clean_text_columns(dataframe, text_columns):
    for col in text_columns:
        dataframe = dataframe.with_columns([
            pd.when(pd.col(col) == 'no definido').then(pd.lit('nodefinido'))
              .when(pd.col(col) == 'no aplica').then(pd.lit('noaplica'))
              .when(pd.col(col) == 'sin descripcion').then(pd.lit('sindescripcion'))
              .otherwise(pd.col(col))
              .str.to_lowercase()
              .alias(col)
        ])
        
        dataframe = dataframe.with_columns([
            pd.col(col).fill_null('').alias(col) for col in dataframe.columns
        ])

        dataframe = dataframe.with_columns([
            pd.col(col).map_elements(remove_extra_punct, return_dtype=pd.Utf8).alias(col)
        ])
        
        dataframe = dataframe.with_columns([
            pd.when(pd.col(col) == 'nodefinido').then(pd.lit('no definido'))
              .when(pd.col(col) == 'noaplica').then(pd.lit('no aplica'))
              .when(pd.col(col) == 'sindescripcion').then(pd.lit('sin descripcion'))
              .otherwise(pd.col(col))
              .str.to_lowercase()
              .alias(col)
        ])
    return dataframe

def clean_numeric_columns(dataframe, numeric_columns):
    values_to_replace = ['', 'unspecified', 'nodefinido', 'sin descripcion']
    for col in numeric_columns:
        for value in values_to_replace:
            dataframe = dataframe.with_columns([pd.col(col).str.replace(value, '0')])

    values_to_replace = ['V1.', 'v1.', '-', '.', ' ']
    for col in numeric_columns:
        for value in values_to_replace:
            dataframe = dataframe.with_columns([pd.col(col).str.replace(value, '')])
    return dataframe

def clean_date(series):
    return series.str.replace('T00:00:00.000', '')

def clean_date_columns(dataframe, date_columns):
    for col in date_columns:
        # Manejar valores nulos o vacíos
        dataframe = dataframe.with_columns([
            pd.when((pd.col(col).is_null()) | (pd.col(col) == ''))
            .then(pd.lit('1900-01-01'))
            .otherwise(pd.col(col))
            .alias(col)
        ])
        
        # Aplicar la función de limpieza a la columna usando 'map_batches'
        dataframe = dataframe.with_columns([
            pd.col(col).map_batches(clean_date).alias(col)
        ])
        
        # Convertir la columna a tipo fecha
        dataframe = dataframe.with_columns([
            pd.col(col).str.strptime(pd.Date, format="%Y-%m-%d", strict=False).alias(col)
        ])
        
    return dataframe

def clean_url(series):
    series = series.str.replace(r"\{ 'url ' : '", "").replace(r" \? numconstancia= ' \}", "").replace(' : ', ':')
    series = series.str.replace(r'\{ "url " : "', "").replace(r' \? numconstancia= " \}', "").replace(' : ', ':')
    series = series.str.replace(r"\{'", "").replace(r"'\}", "")
    series = series.str.replace(r'\{"', "").replace(r'"\}', "")
    series = series.str.replace(r"\{'url': '", "").replace(r"'\}", "")
    series = series.str.replace(r'\{"url": "', "").replace(r'"\}', "")
    return series

def clean_url_columns(dataframe, url_columns):
    for col in url_columns:
        dataframe = dataframe.with_columns([
            pd.col(col).map_batches(clean_url).alias(col)
        ])
        
    return dataframe

def clean_descriptions_batch(series):
    return series.str.replace_all(r'[\x00-\x1F\x7F-\x9F]', '')

def clean_descriptions_columns(dataframe, cols):
    for col in cols:
        dataframe = dataframe.with_columns([
            pd.col(col).map_batches(clean_descriptions_batch).alias(col)
        ])
    return dataframe

def clean_printable_chars(series: pd.Series) -> pd.Series:
    return series.map_elements(lambda x: ''.join([char for char in str(x) if char.isprintable()]), return_dtype=pd.Utf8)

def clean_contracts(data_simplified):
    categorical_columns = ['habilita_pago_adelantado', 'liquidaci_n', 'obligaci_n_ambiental', 'obligaciones_postconsumo', 'reversion', 'estado_bpin', 'anno_bpin', 'espostconflicto']
    text_columns = ['descripcion_del_proceso', 'justificacion_modalidad_de', 'objeto_del_contrato']
    numeric_columns = ['codigo_de_categoria_principal', 'saldo_cdp', 'saldo_vigencia', 'dias_adicionados']
    date_columns = ['fecha_de_firma', 'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato', 'ultima_actualizacion']
    url_columns = ['urlproceso']
    columns_to_cast = []; columns_to_cast.extend(categorical_columns); columns_to_cast.extend(text_columns); columns_to_cast.extend(numeric_columns); columns_to_cast.extend(date_columns); columns_to_cast.extend(url_columns)

    # Inicio del proceso de limpieza y estandarización
    data_simplified['contratos'] = cast_columns(data_simplified['contratos'], columns_to_cast)
    data_simplified['contratos'] = encode_categorical_columns(data_simplified['contratos'], categorical_columns)
    data_simplified['contratos'] = clean_text_columns(data_simplified['contratos'], text_columns)
    data_simplified['contratos'] = clean_numeric_columns(data_simplified['contratos'], numeric_columns)
    data_simplified['contratos'] = clean_date_columns(data_simplified['contratos'], date_columns)
    data_simplified['contratos'] = clean_url_columns(data_simplified['contratos'], url_columns)
    data_simplified['contratos'] = clean_descriptions_columns(data_simplified['contratos'], ['descripcion_del_proceso', 'justificacion_modalidad_de', 'objeto_del_contrato'])
    # Ajuste final para correcto almacenamiento
    for col in data_simplified['contratos'].columns:
        data_simplified['contratos'] = data_simplified['contratos'].with_columns([pd.col(col).map_batches(clean_printable_chars).alias(col)])

def clean_type_ds(data_simplified):
    type_ds = ['orden_entidad', 'sector_entidad', 'rama_entidad', 'estado_contrato', 'tipo_contrato', 'modalidad', 'tipo_documento_proveedor', 'tipo_documento_replegal', 'condiciones_entrega', 'origen_recursos',
               'nacionalidad_replegal', 'genero_replegal', 'destino_gasto', 'punto_acuerdo', 'pilar_acuerdo', 'ubicacion']

    for ds_name in type_ds:
        ds_columns = [col for col in data_simplified[ds_name].columns if col != 'id']
        data_simplified[ds_name] = cast_columns(data_simplified[ds_name], ds_columns)
        data_simplified[ds_name] = clean_text_columns(data_simplified[ds_name], ds_columns)

def clean_replegal(data_simplified):
    text_columns = ['nombre_representante_legal']
    numeric_columns = ['identificaci_n_representante_legal']
    columns_to_cast = []; columns_to_cast.extend(text_columns); columns_to_cast.extend(numeric_columns)

    # Inicio del proceso de limpieza y estandarización
    data_simplified['replegal'] = cast_columns(data_simplified['replegal'], columns_to_cast)
    data_simplified['replegal'] = clean_text_columns(data_simplified['replegal'], text_columns)
    data_simplified['replegal'] = clean_numeric_columns(data_simplified['replegal'], numeric_columns)
    # Ajuste final para correcto almacenamiento
    for col in data_simplified['replegal'].columns:
        data_simplified['replegal'] = data_simplified['replegal'].with_columns([pd.col(col).map_batches(clean_printable_chars).alias(col)])

def clean_entities(data_simplified):
    text_columns = ['nombre_entidad']
    numeric_columns = ['nit_entidad']
    columns_to_cast = []; columns_to_cast.extend(text_columns); columns_to_cast.extend(numeric_columns)

    # Inicio del proceso de limpieza y estandarización
    data_simplified['entidad'] = cast_columns(data_simplified['entidad'], columns_to_cast)
    data_simplified['entidad'] = clean_text_columns(data_simplified['entidad'], text_columns)
    data_simplified['entidad'] = clean_numeric_columns(data_simplified['entidad'], numeric_columns)
    data_simplified['entidad']  = data_simplified['entidad'].with_columns([pd.col("entidad_centralizada").cast(pd.Utf8)])
    data_simplified['entidad']  = data_simplified['entidad'].with_columns([pd.when(pd.col('entidad_centralizada') == 'Centralizada')
                                                                              .then(pd.lit('1'))
                                                                              .otherwise(pd.lit('0'))
                                                                              .alias('entidad_centralizada')
                                                                              ])
    # Ajuste final para correcto almacenamiento
    for col in data_simplified['entidad'].columns:
        data_simplified['entidad'] = data_simplified['entidad'].with_columns([pd.col(col).map_batches(clean_printable_chars).alias(col)])

def clean_providers(data_simplified):
    categorical_columns = ['es_grupo', 'es_pyme']
    text_columns = ['proveedor_adjudicado']
    numeric_columns = ['documento_proveedor']
    columns_to_cast = []; columns_to_cast.extend(text_columns); columns_to_cast.extend(numeric_columns); columns_to_cast.extend(categorical_columns)

    # Inicio del proceso de limpieza y estandarización
    data_simplified['proveedor'] = cast_columns(data_simplified['proveedor'], columns_to_cast)
    data_simplified['proveedor'] = encode_categorical_columns(data_simplified['proveedor'], categorical_columns)
    data_simplified['proveedor'] = clean_text_columns(data_simplified['proveedor'], text_columns)
    data_simplified['proveedor'] = clean_numeric_columns(data_simplified['proveedor'], numeric_columns)
    # Ajuste final para correcto almacenamiento
    for col in data_simplified['proveedor'].columns:
        data_simplified['proveedor'] = data_simplified['proveedor'].with_columns([pd.col(col).map_batches(clean_printable_chars).alias(col)])