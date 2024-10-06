import pandas as pd

class ContractDataProcessor:
    def __init__(self, base_path="../Datos/bronze/"):
        self.base_path = base_path
        self.secopii_contratos = "jbjy-vk9h"
        
    def cast_columns(self, df):
        df = df.astype({
            "nombre_entidad": "string", "nit_entidad": "string", "departamento": "string", "ciudad": "string", "orden": "string",
            "sector": "string", "rama": "string", "entidad_centralizada": "string", "proceso_de_compra": "string", "id_contrato": "string",
            "referencia_del_contrato": "string", "estado_contrato": "string", "codigo_de_categoria_principal": "string", 
            "descripcion_del_proceso": "string", "tipo_de_contrato": "string", "modalidad_de_contratacion": "string", 
            "justificacion_modalidad_de": "string", "fecha_de_firma": "string", "fecha_de_inicio_del_contrato": "string", 
            "fecha_de_fin_del_contrato": "string", "condiciones_de_entrega": "string", "tipodocproveedor": "string", 
            "documento_proveedor": "string", "proveedor_adjudicado": "string", "es_grupo": "string", "es_pyme": "string",
            "habilita_pago_adelantado": "string", "liquidaci_n": "string", "obligaci_n_ambiental": "string", 
            "obligaciones_postconsumo": "string", "reversion": "string", "origen_de_los_recursos": "string", 
            "destino_gasto": "string", "espostconflicto": "string", "valor_del_contrato": "float64", "valor_de_pago_adelantado": "float64", 
            "valor_facturado": "float64", "valor_pendiente_de_pago": "float64", "valor_pagado": "float64", 
            "valor_amortizado": "float64", "valor_pendiente_de": "float64", "valor_pendiente_de_ejecucion": "float64",
            "estado_bpin": "string", "c_digo_bpin": "string", "anno_bpin": "string", "saldo_cdp": "string", "saldo_vigencia": "string",
            "puntos_del_acuerdo": "string", "pilares_del_acuerdo": "string", "dias_adicionados": "string", "urlproceso": "string", 
            "nombre_representante_legal": "string", "nacionalidad_representante_legal": "string", "domicilio_representante_legal": "string", 
            "tipo_de_identificaci_n_representante_legal": "string", "identificaci_n_representante_legal": "string", 
            "g_nero_representante_legal": "string", "sistema_general_de_participaciones": "string", "sistema_general_de_regal_as": "string", 
            "recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_": "string", "recursos_de_credito": "string", 
            "recursos_propios": "string", "ultima_actualizacion": "string", "codigo_entidad": "string", "codigo_proveedor": "string", 
            "objeto_del_contrato": "string", "presupuesto_general_de_la_nacion_pgn": "string"
        })
        df["estado_contrato"] = df["estado_contrato"].str.lower()
        return df

    def load_historic_by_year(self, id_contract_column='id_contrato', year=2024):
        file_path = f'{self.base_path}{year}/S2.csv'
        historic = pd.read_csv(file_path, sep=';', error_bad_lines=False, encoding='utf-8')
        historic = self.cast_columns(historic)
        return historic.drop_duplicates(subset=[id_contract_column])

    def basic_query_s2(self, year):
        query = f"""
            SELECT 
                nombre_entidad, nit_entidad, departamento, ciudad, orden, sector, rama, entidad_centralizada, proceso_de_compra, id_contrato, referencia_del_contrato,
                estado_contrato, codigo_de_categoria_principal, descripcion_del_proceso, tipo_de_contrato, modalidad_de_contratacion, justificacion_modalidad_de, fecha_de_firma, 
                fecha_de_inicio_del_contrato, fecha_de_fin_del_contrato, condiciones_de_entrega, tipodocproveedor, documento_proveedor, proveedor_adjudicado, es_grupo, 
                es_pyme, habilita_pago_adelantado, liquidaci_n, obligaci_n_ambiental, obligaciones_postconsumo, reversion, origen_de_los_recursos, destino_gasto, valor_del_contrato,
                valor_de_pago_adelantado, valor_facturado, valor_pendiente_de_pago, valor_pagado, valor_amortizado, valor_pendiente_de, valor_pendiente_de_ejecucion, estado_bpin,
                c_digo_bpin, anno_bpin, saldo_cdp, saldo_vigencia, espostconflicto, dias_adicionados, puntos_del_acuerdo, pilares_del_acuerdo, urlproceso, nombre_representante_legal,
                nacionalidad_representante_legal, domicilio_representante_legal, tipo_de_identificaci_n_representante_legal, identificaci_n_representante_legal, g_nero_representante_legal,
                presupuesto_general_de_la_nacion_pgn, sistema_general_de_participaciones, sistema_general_de_regal_as, recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_, 
                recursos_de_credito, recursos_propios, ultima_actualizacion, codigo_entidad, codigo_proveedor, objeto_del_contrato
            WHERE
                fecha_de_firma >= '{year}-01-01' and fecha_de_firma < '{year+1}-01-01'
            LIMIT
                1000000000000
        """
        return query

    def load_last_view_by_year(self, client, year=2024, id_contract_column='id_contrato'):
        last_view = client.get(self.secopii_contratos, content_type="json", query=self.basic_query_s2(year))
        last_view = pd.DataFrame(last_view)
        last_view = self.cast_columns(last_view)
        return last_view.drop_duplicates(subset=id_contract_column, keep='first')

    def identify_new_contracts(self, historic: pd.DataFrame, last_view: pd.DataFrame, id_contract_column: str = 'id_contrato'):
        # Convertir las columnas de identificación a conjuntos para una búsqueda eficiente
        historic_ids = set(historic[id_contract_column])
        last_view_ids = set(last_view[id_contract_column])
        
        # Identificar nuevos contratos
        new_contracts_ids = last_view_ids - historic_ids
        new_contracts = last_view[last_view[id_contract_column].isin(new_contracts_ids)]
        
        if len(new_contracts) > 0:
            print(f'-Se identificaron {len(new_contracts)} nuevos contratos.')
            # Concatenar DataFrames de manera eficiente
            historic = pd.concat([historic, new_contracts])
        
        return new_contracts[id_contract_column].unique(), historic

    def exclude_new_contracts(self, historic, last_view, new_contracts_by_year):
        new_contracts_by_year_set = set(new_contracts_by_year)
        historic_contracts_without_new_contracts = historic[~historic['id_contrato'].isin(new_contracts_by_year_set)]
        last_view_without_new_contracts = last_view[~last_view['id_contrato'].isin(new_contracts_by_year_set)]
        return historic_contracts_without_new_contracts, last_view_without_new_contracts

    def validate_amount_of_contracts(self, historic, last_view, new_contracts_by_year, warnings_by_year):
        historic_contracts_without_new_contracts, last_view_without_new_contracts = self.exclude_new_contracts(historic, last_view, new_contracts_by_year)
        if len(historic_contracts_without_new_contracts) != len(last_view_without_new_contracts):
            warnings_by_year['cantidad_contratos'] = f'Histórico: {len(historic_contracts_without_new_contracts)} - Última revisión: {len(last_view_without_new_contracts)}'

    def validate_value_of_contracts(self, historic, last_view, new_contracts_by_year, warnings_by_year, value_contracts_column):
        historic_contracts_without_new_contracts, last_view_without_new_contracts = self.exclude_new_contracts(historic, last_view, new_contracts_by_year)
        if historic_contracts_without_new_contracts.shape[0] != last_view_without_new_contracts.shape[0]:
            historic_sum = historic_contracts_without_new_contracts[value_contracts_column].sum()
            last_view_sum = last_view_without_new_contracts[value_contracts_column].sum()
            warnings_by_year[value_contracts_column] = f'Histórico: {historic_sum} - Última revisión: {last_view_sum}'
