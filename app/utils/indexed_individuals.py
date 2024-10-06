import polars as pd

class indexed_individuals:
    def __init__(self, start_id=0, historical_individuals_path=None, data=None, individuals_columns=None):
        """
        Inicializa la clase con un DataFrame para individuos históricos y un ID inicial.
        Si se proporciona historical_individuals, se usará como el DataFrame de históricos.
        """
        self.historical_individuals_path = historical_individuals_path
        self.historical_individuals = pd.read_csv(historical_individuals_path, separator=';', ignore_errors=True) if historical_individuals_path is not None else pd.DataFrame()
        self.current_id = start_id if self.historical_individuals is None else self.extract_current_id(self.historical_individuals)
        self.data = data if data is not None else pd.DataFrame()
        self.individuals_columns = list(self.historical_individuals.columns) if self.historical_individuals is not None else individuals_columns
        if 'id' in self.individuals_columns:
            self.individuals_columns.remove('id')

    def assign_ids(self, df, id_column='id'):
        """
        Asigna identificadores únicos a un DataFrame, comenzando desde el ID actual.
        """
        max_id = self.current_id if id_column not in df.columns or df[id_column].isnull().all() else int(df[id_column].max() + 1)
        df = df.with_columns([pd.Series(id_column, range(max_id, max_id + len(df)))])
        self.current_id = max_id + len(df)
        return df

    def identify_existing_individuals(self):
        """
        Identifica nuevos individuos comparando el DataFrame reciente con el histórico.
        """
        if self.historical_individuals.is_empty():
            return self.data.select(self.individuals_columns)

        # Realizamos el join para encontrar individuos existentes
        self.data = self.data.with_columns([pd.col(col).cast(pd.String) for col in self.individuals_columns])
        self.historical_individuals = self.historical_individuals.with_columns([pd.col(col).cast(pd.String) for col in self.individuals_columns])

        # Realiza el merge
        merged_individuals = self.data.join(self.historical_individuals, on=self.individuals_columns, how='left')

        # Verificamos que la columna de la histórica esté presente
        historical_column = self.historical_individuals.columns[0]
        if historical_column not in merged_individuals.columns:
            print(f"Error: La columna '{historical_column}' no se encuentra en el DataFrame resultante del join.")
            return pd.DataFrame([])

        # Filtramos los nuevos individuos
        new_individuals = merged_individuals.filter(pd.col(historical_column).is_null())

        # Seleccionamos las columnas originales sin el sufijo '_existing'
        new_individuals = new_individuals.select(self.individuals_columns)

        return new_individuals

    def update_existing_individuals(self):
        """
        Actualiza el DataFrame histórico con nuevos individuos, asignándoles identificadores únicos si es necesario.
        """
        new_individuals = self.identify_existing_individuals()
        if not new_individuals.is_empty():
            new_individuals = self.assign_ids(new_individuals)
            new_individuals = new_individuals.select(self.historical_individuals.columns)
            self.historical_individuals = pd.concat([self.historical_individuals, new_individuals])
        self.historical_individuals.write_csv(self.historical_individuals_path, separator=';')

    def extract_current_id(self, df):
        """
        Extrae el ID máximo actual del DataFrame, asignando IDs si no existen.
        """
        if 'id' not in df.columns:
            df = self.assign_ids(df)
        return int(df['id'].max() + 1)

    def update_data(self, new_id_column_name='id'):
        """
        Elimina columnas de individuos del DataFrame histórico después de asociarles identificadores únicos.
        """
        new_data = self.data.join(
            self.historical_individuals, on=self.individuals_columns, how='left'
        )
        new_data = new_data.rename({'id': new_id_column_name}).drop(self.individuals_columns)
        self.data = new_data