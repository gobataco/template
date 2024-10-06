import polars as pd
from fuzzywuzzy import fuzz
from collections import defaultdict
import utils.indexed_individuals as ii
from rapidfuzz.distance import Levenshtein
import os

def simplify_data_to_clean(last_view):
    data_simplified = {}

    # Ubicaciones (Departamento y municipio)
    index_manager = ii.indexed_individuals(data=last_view, historical_individuals_path='../data/bronze/auxiliar/ubicacion.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_ubicacion')
    data_simplified['ubicacion'] = index_manager.historical_individuals

    # Orden de la entidades
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/orden_entidad.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_orden_entidad')
    data_simplified['orden_entidad'] = index_manager.historical_individuals

    # Sector de la entidad
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/sector_entidad.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_sector_entidad')
    data_simplified['sector_entidad'] = index_manager.historical_individuals

    # Rama de la entidad
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/rama_entidad.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_rama_entidad')
    data_simplified['rama_entidad'] = index_manager.historical_individuals

    # Estado de contrato
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/estado_contrato.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_estado_contrato')
    data_simplified['estado_contrato'] = index_manager.historical_individuals

    # Tipo de contrato
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/tipo_contrato.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_tipo_contrato')
    data_simplified['tipo_contrato'] = index_manager.historical_individuals

    # Modalidad de contratación
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/modalidad_contratacion.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_modalidad')
    data_simplified['modalidad'] = index_manager.historical_individuals

    # # Tipo documento proveedor
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/tipo_documento.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_tipo_documento_proveedor')
    data_simplified['tipo_documento_proveedor'] = index_manager.historical_individuals

    # Tipo documento representante legal
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/tipo_documento_replegal.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_tipo_documento_replegal')
    data_simplified['tipo_documento_replegal'] = index_manager.historical_individuals

    # Condiciones de entrega
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/condiciones_entrega.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_condicion_entrega')
    data_simplified['condiciones_entrega'] = index_manager.historical_individuals

    # Origen de recursos
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/origen_recursos.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_origen_recursos')
    data_simplified['origen_recursos'] = index_manager.historical_individuals

    # Nacionalidad del representante legal
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/nacionalidad_replegal.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_nacionalidad_replegal')
    data_simplified['nacionalidad_replegal'] = index_manager.historical_individuals

    # Genero replegal
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/genero_replegal.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_genero_replegal')
    data_simplified['genero_replegal'] = index_manager.historical_individuals

    # Representante legal de la entidad
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/replegal.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_replegal')
    data_simplified['replegal'] = index_manager.historical_individuals

    # Entidades
    index_manager.data.with_columns([pd.col('nit_entidad').cast(pd.String), pd.col('codigo_entidad').cast(pd.String)])
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/entidad.csv')
    index_manager.historical_individuals.with_columns([pd.col('nit_entidad').cast(pd.String), pd.col('codigo_entidad').cast(pd.String)])
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_entidad')
    data_simplified['entidad'] = index_manager.historical_individuals

    # Proveedores
    index_manager.data.with_columns([pd.col('codigo_proveedor').cast(pd.String)])
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/proveedor.csv')
    index_manager.historical_individuals.with_columns([pd.col('codigo_proveedor').cast(pd.String)])
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_proveedor')
    data_simplified['proveedor'] = index_manager.historical_individuals

    # Destino gasto
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/destino_gasto.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_destino_gasto')
    data_simplified['destino_gasto'] = index_manager.historical_individuals

    # Puntos del acuerdo
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/puntos_acuerdo.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_punto_acuerdo')
    data_simplified['punto_acuerdo'] = index_manager.historical_individuals

    # Pilares del acuerdo
    index_manager = ii.indexed_individuals(data=index_manager.data, historical_individuals_path='../data/bronze/auxiliar/pilar_acuerdo.csv')
    index_manager.update_existing_individuals()
    index_manager.update_data(new_id_column_name='id_pilar_acuerdo')
    data_simplified['pilar_acuerdo'] = index_manager.historical_individuals
    data_simplified['contratos'] = index_manager.data

    return data_simplified

def prepare_data(data, nit_column_name, name_column_name):
    data_prepared = data[[nit_column_name, name_column_name]]
    data_prepared = data_prepared.drop_duplicates()
    data_prepared[nit_column_name] = data_prepared[nit_column_name].astype(str)
    data_prepared = data_prepared.rename({nit_column_name: 'document'})
    return data_prepared

class combined_cluster_manager:
    def __init__(self, df_clusters_nits=None, df_clusters_names=None):
        self.clusters = defaultdict(list)
        self.representative_nit = {}
        self.representative_name = {}
        self.existing_entities = set()
        
        if df_clusters_nits is not None and df_clusters_nits.shape[0] > 0:
            self.initialize_from_dataframe_nits(df_clusters_nits)
        if df_clusters_names is not None and df_clusters_names.shape[0] > 0:
            self.initialize_from_dataframe_names(df_clusters_names)
        else:
            self.next_cluster_id = 1

    def initialize_from_dataframe_nits(self, df: pd.DataFrame):
        """Inicializa los clusters a partir de un DataFrame existente basado en NITs."""
        cluster_ids = df['cluster_id'].to_list()
        nits = df['document'].to_list()

        for cluster_id, nit in zip(cluster_ids, nits):
            if cluster_id not in self.clusters:
                self.clusters[cluster_id] = []
            self.clusters[cluster_id].append(nit)
            self.representative_nit[cluster_id] = nit
            self.existing_entities.add((nit, None))

    def initialize_from_dataframe_names(self, df: pd.DataFrame):
        """Inicializa los clusters a partir de un DataFrame existente basado en nombres."""
        cluster_ids = df['cluster_id'].to_list()
        names = df['name'].to_list()

        for cluster_id, name in zip(cluster_ids, names):
            if cluster_id not in self.clusters:
                self.clusters[cluster_id] = []
            self.clusters[cluster_id].append(name)
            self.representative_name[cluster_id] = name
            self.existing_entities.add((None, name))

    def find_cluster(self, nit, name):
        """Encuentra el cluster más adecuado para una entidad basado en NIT y nombre."""
        for cluster_id, members in self.clusters.items():
            rep_nit = self.representative_nit.get(cluster_id, None)
            rep_name = self.representative_name.get(cluster_id, None)
            nit_similarity = fuzz.ratio(str(nit), str(rep_nit)) if rep_nit else 0

            # Si el nombre contiene la palabra "regional", solo consideramos el NIT
            if "regional" in name or 'sena' in name or 'icbf' in name or 'dane' in name:
                if nit_similarity > 89:
                    return cluster_id
            else:
                # Usando Levenshtein distance para la similitud de nombres
                if rep_name:
                    max_len = max(len(name), len(rep_name))
                    name_similarity = 1 - (Levenshtein.distance(name, rep_name) / max_len)
                else:
                    name_similarity = 0

                if nit_similarity > 89 and name_similarity > 0.35:  # Ajusta el umbral según sea necesario
                    return cluster_id

        return None

    def add_entity(self, nit, name):
        """Agrega una entidad a un cluster si no está ya presente en el conjunto de entidades existentes."""
        if (nit, name) not in self.existing_entities:
            cluster_id = self.find_cluster(nit, name)
            if cluster_id is not None:
                self.clusters[cluster_id].append((nit, name))
            else:
                self.clusters[self.next_cluster_id].append((nit, name))
                self.representative_nit[self.next_cluster_id] = nit
                self.representative_name[self.next_cluster_id] = name
                self.next_cluster_id += 1
            self.existing_entities.add((nit, name))

    def get_clusters_as_dataframe(self):
        """Devuelve los clusters como un DataFrame de Polars para análisis o manipulación fácil."""
        clustered_data = []
        for cluster_id, members in self.clusters.items():
            for member in members:
                nit, name = member
                clustered_data.append({'cluster_id': cluster_id, 'document': nit, 'name': name})
        return pd.DataFrame(clustered_data)

# Función para cargar o crear archivos de clusterización
def load_or_create_clusters(filepath, column_names):
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, separator=';')
    else:
        # Crear un DataFrame vacío con las columnas necesarias si no existe el archivo
        df = pd.DataFrame(schema={name: pd.Utf8 for name in column_names})
        df.write_csv(filepath, separator=';')
    return df

# Ejemplo de uso con NITs y nombres de entidades
def clustering_entities_combined(data_simplified):
    entity_key_df = data_simplified['entidad'].select(['nit_entidad', 'nombre_entidad']).unique().rename({
        'nit_entidad': 'document',
        'nombre_entidad': 'name'
    })
    
    # Carga de clusters previos desde archivos CSV, o creación si no existen
    entity_clusterized_nits = load_or_create_clusters('../data/gold/auxiliar/entity_clusterized_nits.csv', ['cluster_id', 'document'])
    entity_clusterized_names = load_or_create_clusters('../data/gold/auxiliar/entity_clusterized_names.csv', ['cluster_id', 'name'])
    
    # Inicialización del gestor de clusters combinados
    cluster_manager = combined_cluster_manager(entity_clusterized_nits, entity_clusterized_names)
    
    for document, name in entity_key_df.iter_rows():
        cluster_manager.add_entity(document, name)

    df_updated_clusters = cluster_manager.get_clusters_as_dataframe()
    
    # Se actualiza el archivo CSV de clusters combinados
    df_updated_clusters.write_csv('../data/gold/auxiliar/entity_clusterized_combined.csv', separator=';')
    
    # Unión de los datos clusterizados con el DataFrame original
    entity_key_df = entity_key_df.join(df_updated_clusters, on=['document', 'name'], how='left')
    entity_key_df = entity_key_df.unique()
    entity_key_df = entity_key_df.rename({
        'document': 'nit_entidad',
        'name': 'nombre_entidad',
        'cluster_id': 'id_entidad'
    })
    
    # Inclusión de la nueva variable en los datos existentes
    final_join = data_simplified['entidad'].join(entity_key_df, on=['nit_entidad', 'nombre_entidad'], how='left').unique()
    return final_join

def ajust_locations(data_simplified, maestro_municipios):
    ubicacion = data_simplified['ubicacion']
    
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('bogota') | pd.col('departamento').str.contains('bogota')).then(pd.lit('bogota . d.c .')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('bogota') | pd.col('departamento').str.contains('bogota')).then(pd.lit('bogota . d.c .')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('chiquiza') & pd.col('departamento').str.contains('boyaca')).then(pd.lit('chiquiza')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('ariguani') & pd.col('departamento').str.contains('magdalena')).then(pd.lit('ariguani')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('paez') & pd.col('departamento').str.contains('cauca')).then(pd.lit('paez')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('playa') & pd.col('departamento').str.contains('norte sandander')).then(pd.lit('playa')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('purisima') & pd.col('departamento').str.contains('cordoba')).then(pd.lit('purisima concepcion')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('san miguel') & pd.col('departamento').str.contains('putumayo')).then(pd.lit('san miguel')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('villa garzon') & pd.col('departamento').str.contains('putumayo')).then(pd.lit('villagarzon')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('tolu viejo') & pd.col('departamento').str.contains('sucre')).then(pd.lit('san jose toluviejo')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('tumaco') & pd.col('departamento').str.contains('nariño')).then(pd.lit('san andres tumaco')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('silos') & pd.col('departamento').str.contains('norte sandander')).then(pd.lit('silos')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('carolina') & pd.col('departamento').str.contains('antioquia')).then(pd.lit('carolina')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('salazar') & pd.col('departamento').str.contains('norte sandander')).then(pd.lit('salazar')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('mariquita') & pd.col('departamento').str.contains('tolima')).then(pd.lit('san sebastian mariquita')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('mompos') & pd.col('departamento').str.contains('bolivar')).then(pd.lit('santa cruz mompox')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('cartagena') & pd.col('departamento').str.contains('bolivar')).then(pd.lit('cartagena indias')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('cucuta') & pd.col('departamento').str.contains('norte santander')).then(pd.lit('san jose cucuta')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('valle guamuez') & pd.col('departamento').str.contains('putumayo')).then(pd.lit('valle guamuez')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('santafe antioquia') & pd.col('departamento').str.contains('antioquia')).then(pd.lit('santa fe antioquia')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('cuaspud') & pd.col('departamento').str.contains('nariño')).then(pd.lit('cuaspud carlosama')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('barranco minas') & pd.col('departamento').str.contains('guainia')).then(pd.lit('barrancominas')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('chibolo') & pd.col('departamento').str.contains('magdalena')).then(pd.lit('chivolo')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('san vicente') & pd.col('departamento').str.contains('antioquia')).then(pd.lit('san vicente ferrer')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('don matias') & pd.col('departamento').str.contains('antioquia')).then(pd.lit('donmatias')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('roberto payan') & pd.col('departamento').str.contains('nariño')).then(pd.lit('roberto payan')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('san estanislao') & pd.col('departamento').str.contains('bolivar')).then(pd.lit('san estanislao')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('since') & pd.col('departamento').str.contains('sucre')).then(pd.lit('san luis since')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('güican') & pd.col('departamento').str.contains('boyaca')).then(pd.lit('güican sierra')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('san juan rio seco') & pd.col('departamento').str.contains('cundinamarca')).then(pd.lit('san juan rioseco')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('bajo baudo') & pd.col('departamento').str.contains('choco')).then(pd.lit('bajo baudo')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('sotara') & pd.col('departamento').str.contains('cauca')).then(pd.lit('sotara paispamba')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('medio san juan') & pd.col('departamento').str.contains('choco')).then(pd.lit('medio san juan')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('leguizamo') & pd.col('departamento').str.contains('putumayo')).then(pd.lit('puerto leguizamo')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('lopez') & pd.col('departamento').str.contains('cauca')).then(pd.lit('lopez micay')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('manaure') & pd.col('departamento').str.contains('cesar')).then(pd.lit('manaure balon cesar')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('silos') & pd.col('departamento').str.contains('norte santander')).then(pd.lit('silos')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col('ciudad').str.contains('itagui') & pd.col('departamento').str.contains('antioquia')).then(pd.lit('itagúi')).otherwise(pd.col('ciudad')).alias('ciudad')])
    ubicacion = ubicacion.with_columns([pd.when(pd.col("departamento").str.contains("san andres providencia santa catalina")).then(pd.lit("archipielago san andres . providencia santa catalina"))
                                        .otherwise(pd.col("departamento")).alias("departamento")])

    location_ajusted_i1 = ubicacion.join(maestro_municipios, left_on=["departamento", "ciudad"], right_on=["DEPARTAMENTO_LIMPIO", "MUNICIPIO_LIMPIO"], how="left")
    location_ajusted_i1 = location_ajusted_i1.with_columns([
        pd.when(location_ajusted_i1["departamento"].is_null() & location_ajusted_i1["ciudad"].is_null()).then(pd.lit("left_only")).otherwise(pd.lit("both")).alias("_merge")
    ])
    # Intento primer cruce (municipio y departamento)
    location_ajusted_merge_failed = location_ajusted_i1.filter(pd.col('_merge') == 'left_only')
    location_ajusted = location_ajusted_i1.filter(pd.col('_merge') == 'both')
    location_ajusted = location_ajusted.drop('_merge')
    location_ajusted_merge_failed = location_ajusted_merge_failed.drop(['CODIGO_DEPARTAMENTO', 'CODIGO_MUNICIPIO', 'DEPARTAMENTO', 'MUNICIPIO', 'TIPO', 'CATEGORIA_MUNICIPIO', '_merge'])
    # Intento segundo cruce (departamento)
    maestro_departamentos = maestro_municipios.select(['CODIGO_DEPARTAMENTO', 'DEPARTAMENTO', 'DEPARTAMENTO_LIMPIO']).unique()
    maestro_departamentos = maestro_departamentos.with_columns([
        pd.lit(0).alias('CODIGO_MUNICIPIO'),
        pd.lit('NO APLICA').alias('MUNICIPIO'),
        pd.lit('Departamento').alias('TIPO'),
        pd.lit(0).alias('CATEGORIA_MUNICIPIO')
    ])

    location_ajusted_i2 = location_ajusted_merge_failed.join(maestro_departamentos, left_on=["departamento"], right_on=["DEPARTAMENTO_LIMPIO"], how="left")
    if len(location_ajusted_i2) > 0:
        location_ajusted_i2 = location_ajusted_i2.with_columns([pd.col(col).cast(pd.Int64) for col in ['id', 'CODIGO_DEPARTAMENTO', 'CODIGO_MUNICIPIO']])
        location_ajusted_i2 = location_ajusted_i2.with_columns([pd.col(col).cast(pd.Float64) for col in ['CATEGORIA_MUNICIPIO']]).select(location_ajusted.columns)
        data_simplified['ubicacion'] = pd.concat([location_ajusted, location_ajusted_i2])
    else:
        data_simplified['ubicacion'] = location_ajusted
    data_simplified['ubicacion'] = data_simplified['ubicacion'].drop(['departamento', 'ciudad'])
    data_simplified['ubicacion'] = data_simplified['ubicacion'].rename({'CODIGO_DEPARTAMENTO': 'codigo_departamento', 'CODIGO_MUNICIPIO': 'codigo_municipio', 'DEPARTAMENTO': 'departamento', 'MUNICIPIO': 'municipio', 
                                                                        'TIPO': 'tipo', 'CATEGORIA_MUNICIPIO': 'categoria_municipio'})