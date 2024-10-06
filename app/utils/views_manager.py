import polars as pd

# Creación de la vista de contratos
def create_contracts_view(data_simplified):
    contracts_view = data_simplified['contratos'].with_columns([pd.col(col).cast(pd.String) for col in ['id_estado_contrato', 'id_tipo_contrato', 'id_modalidad', 'id_condicion_entrega', 'id_origen_recursos', 
                                                                                                        'id_destino_gasto', 'id_punto_acuerdo', 'id_pilar_acuerdo', 'id_entidad']])
    # Estado del contrato
    data_simplified['estado_contrato'] = data_simplified['estado_contrato'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['estado_contrato'], how='left', left_on='id_estado_contrato', right_on='id')
    contracts_view = contracts_view.drop(['id_estado_contrato'])
    # Tipo del contrato
    data_simplified['tipo_contrato'] = data_simplified['tipo_contrato'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['tipo_contrato'], how='left', left_on='id_tipo_contrato', right_on='id')
    contracts_view = contracts_view.drop(['id_tipo_contrato'])
    # Modalidad de contratación
    data_simplified['modalidad'] = data_simplified['modalidad'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['modalidad'], how='left', left_on='id_modalidad', right_on='id')
    contracts_view = contracts_view.drop(['id_modalidad'])
    # Condiciones de entrega
    data_simplified['condiciones_entrega'] = data_simplified['condiciones_entrega'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['condiciones_entrega'], how='left', left_on='id_condicion_entrega', right_on='id')
    contracts_view = contracts_view.drop(['id_condicion_entrega'])
    # Origen de recursos
    data_simplified['origen_recursos'] = data_simplified['origen_recursos'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['origen_recursos'], how='left', left_on='id_origen_recursos', right_on='id')
    contracts_view = contracts_view.drop(['id_origen_recursos'])
    # Destino de gasto
    data_simplified['destino_gasto'] = data_simplified['destino_gasto'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['destino_gasto'], how='left', left_on='id_destino_gasto', right_on='id')
    contracts_view = contracts_view.drop(['id_destino_gasto'])
    # Puntos del acuerdo
    data_simplified['punto_acuerdo'] = data_simplified['punto_acuerdo'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['punto_acuerdo'], how='left', left_on='id_punto_acuerdo', right_on='id')
    contracts_view = contracts_view.drop(['id_punto_acuerdo'])
    # Pilar del acuerdo
    data_simplified['pilar_acuerdo'] = data_simplified['pilar_acuerdo'].with_columns([pd.col('id').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['pilar_acuerdo'], how='left', left_on='id_pilar_acuerdo', right_on='id')
    contracts_view = contracts_view.drop(['id_pilar_acuerdo'])
    # Entidad
    contracts_view = contracts_view.rename({'id_entidad': 'id_configuracion_entidad'})
    data_simplified['entidad'] = data_simplified['entidad'].rename({'id': 'id_configuracion_entidad'})
    data_simplified['entidad'] = data_simplified['entidad'].with_columns([pd.col('id_configuracion_entidad').cast(pd.String)])
    contracts_view = contracts_view.join(data_simplified['entidad'], how='left', on='id_configuracion_entidad')
    contracts_view = contracts_view.drop(['nit_entidad', 'nombre_entidad', 'codigo_entidad', 'entidad_centralizada', 'id_ubicacion', 'id_orden_entidad', 'id_sector_entidad', 'id_rama_entidad', 'id_replegal'])
    
    contracts_view = contracts_view[['proceso_de_compra', 'id_contrato', 'referencia_del_contrato', 'codigo_de_categoria_principal', 'descripcion_del_proceso', 'justificacion_modalidad_de', 'fecha_de_firma', 
                                     'fecha_de_inicio_del_contrato', 'fecha_de_fin_del_contrato', 'habilita_pago_adelantado', 'liquidaci_n', 'obligaci_n_ambiental', 'obligaciones_postconsumo', 'reversion', 
                                     'valor_del_contrato', 'valor_de_pago_adelantado', 'valor_facturado', 'valor_pendiente_de_pago', 'valor_pagado', 'valor_amortizado', 'valor_pendiente_de', 
                                     'valor_pendiente_de_ejecucion', 'estado_bpin', 'c_digo_bpin', 'anno_bpin', 'saldo_cdp', 'saldo_vigencia', 'espostconflicto', 'dias_adicionados', 'urlproceso', 
                                     'presupuesto_general_de_la_nacion_pgn', 'sistema_general_de_participaciones', 'sistema_general_de_regal_as', 'recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_', 
                                     'recursos_de_credito', 'recursos_propios', 'ultima_actualizacion', 'objeto_del_contrato', 'estado_contrato', 'tipo_de_contrato', 'modalidad_de_contratacion', 
                                     'condiciones_de_entrega', 'origen_de_los_recursos', 'destino_gasto', 'puntos_del_acuerdo', 'pilares_del_acuerdo', 'id_entidad', 'id_configuracion_entidad', 'id_proveedor']]
    
    return contracts_view

def create_entities_view(data_simplified):
    # Creación de la vista de entidades
    entities_view = data_simplified['entidad'].with_columns([pd.col(col).cast(pd.String) for col in ['id_ubicacion', 'id_orden_entidad', 'id_sector_entidad', 'id_rama_entidad', 'id_replegal']])
    # Códigos DIVIPOLA
    data_simplified['ubicacion'] = data_simplified['ubicacion'].with_columns([pd.col('id').cast(pd.String)])
    entities_view = entities_view.join(data_simplified['ubicacion'], how='left', left_on='id_ubicacion', right_on='id')
    entities_view = entities_view.drop(['id_ubicacion', 'departamento', 'municipio', 'tipo', 'categoria_municipio'])
    # Orden
    data_simplified['orden_entidad'] = data_simplified['orden_entidad'].with_columns([pd.col('id').cast(pd.String)])
    entities_view = entities_view.join(data_simplified['orden_entidad'], how='left', left_on='id_orden_entidad', right_on='id')
    entities_view = entities_view.drop(['id_orden_entidad'])
    # Sector
    data_simplified['sector_entidad'] = data_simplified['sector_entidad'].with_columns([pd.col('id').cast(pd.String)])
    entities_view = entities_view.join(data_simplified['sector_entidad'], how='left', left_on='id_sector_entidad', right_on='id')
    entities_view = entities_view.drop(['id_sector_entidad'])
    # Rama
    data_simplified['rama_entidad'] = data_simplified['rama_entidad'].with_columns([pd.col('id').cast(pd.String)])
    entities_view = entities_view.join(data_simplified['rama_entidad'], how='left', left_on='id_rama_entidad', right_on='id')
    entities_view = entities_view.drop(['id_rama_entidad'])

    entities_view = entities_view[['id_entidad', 'id_configuracion_entidad', 'nit_entidad', 'nombre_entidad', 'codigo_entidad', 'entidad_centralizada', 'codigo_departamento', 'codigo_municipio', 'orden', 'sector', 
                                   'rama', 'id_replegal']]
    return entities_view

def create_providers_view(data_simplified):
    # Creación de la vista de proveedores
    providers_view = data_simplified['proveedor'].with_columns([pd.col(col).cast(pd.String) for col in ['id_tipo_documento_proveedor']])
    providers_view = providers_view.rename({'id': 'id_proveedor'})
    # Tipo de documento
    data_simplified['tipo_documento_proveedor'] = data_simplified['tipo_documento_proveedor'].with_columns([pd.col('id').cast(pd.String)])
    providers_view = providers_view.join(data_simplified['tipo_documento_proveedor'], how='left', left_on='id_tipo_documento_proveedor', right_on='id')
    providers_view = providers_view.drop(['id_tipo_documento_proveedor'])
    
    providers_view = providers_view[['id_proveedor', 'tipodocproveedor', 'documento_proveedor', 'proveedor_adjudicado', 'codigo_proveedor', 'es_grupo', 'es_pyme']]
    return providers_view

def create_replegal_view(data_simplified):
    # Creación de la vista de representantes legales
    rplegal_view = data_simplified['replegal'].with_columns([pd.col(col).cast(pd.String) for col in ['id_nacionalidad_replegal', 'id_tipo_documento_replegal', 'id_genero_replegal']])
    rplegal_view = rplegal_view.rename({'id': 'id_representante_legal'})
    # Tipo de documento
    data_simplified['tipo_documento_replegal'] = data_simplified['tipo_documento_replegal'].with_columns([pd.col('id').cast(pd.String)])
    rplegal_view = rplegal_view.join(data_simplified['tipo_documento_replegal'], how='left', left_on='id_tipo_documento_replegal', right_on='id')
    rplegal_view = rplegal_view.drop(['id_tipo_documento_replegal'])
    # Nacionalidad
    data_simplified['nacionalidad_replegal'] = data_simplified['nacionalidad_replegal'].with_columns([pd.col('id').cast(pd.String)])
    rplegal_view = rplegal_view.join(data_simplified['nacionalidad_replegal'], how='left', left_on='id_nacionalidad_replegal', right_on='id')
    rplegal_view = rplegal_view.drop(['id_nacionalidad_replegal'])
    # Genero
    data_simplified['genero_replegal'] = data_simplified['genero_replegal'].with_columns([pd.col('id').cast(pd.String)])
    rplegal_view = rplegal_view.join(data_simplified['genero_replegal'], how='left', left_on='id_genero_replegal', right_on='id')
    rplegal_view = rplegal_view.drop(['id_genero_replegal'])
    
    rplegal_view = rplegal_view[['id_representante_legal', 'tipo_de_identificaci_n_representante_legal', 'identificaci_n_representante_legal', 'nombre_representante_legal', 'domicilio_representante_legal', 
                                 'nacionalidad_representante_legal', 'g_nero_representante_legal']]
    return rplegal_view