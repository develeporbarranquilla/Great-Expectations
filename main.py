# Importar las librerías necesarias
import great_expectations as gx
import pandas as pd

# Evitar que se muestren advertencias
import warnings
warnings.filterwarnings("ignore")

from great_expectations.checkpoint import (    
    UpdateDataDocsAction
)

# Crea un contexto de datos en un directorio específico para alamacenar la documentación
context = gx.get_context(mode="file", project_root_dir="./gx_contexto_detalle_suite")

# Opcional. Se imprime la configuración del contexto
print(context)

# Nombre de la suite de expectativas.  La suite contiene las expectativas que se desean aplicar a los datos.
suite_name = "my_expectation_suite"

# Como se almacenca la suite de expectativas en un directorio específico, se verifica si ya existe
try:
    suite = context.suites.get(name=suite_name)
except gx.exceptions.exceptions.DataContextError:
    # Si no existe, se crea una nueva suite
    suite = gx.ExpectationSuite(name=suite_name)
    # Se agrega la suite al contexto
    suite = context.suites.add(suite)

# Creación de las expectativas
expectation_1 = gx.expectations.ExpectColumnValuesToNotBeNull(column="Proveedor")
expectation_2 = gx.expectations.ExpectColumnValuesToBeOfType(column='Posición', type_='int64')
expectation_3 = gx.expectations.ExpectColumnValuesToMatchRegex(column='Nombre del usuario',regex="^[A-Za-z]+$" )
expectation_4 = gx.expectations.ExpectColumnDistinctValuesToBeInSet(column='Centro',value_set=['VSA', 'BOG', 'CCA'])
expectation_5 = gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(column="Fe.contabilización",strftime_format="%d/%m/%Y")
expectation_6 = gx.expectations.ExpectColumnValuesToBeUnique(
    column="Documento material"
)

# Se agregan las expectativas a la suite
suite.add_expectation(expectation=expectation_1)
suite.add_expectation(expectation=expectation_2)
suite.add_expectation(expectation=expectation_3)
suite.add_expectation(expectation=expectation_4)
suite.add_expectation(expectation=expectation_5)
suite.add_expectation(expectation=expectation_6)

# Definición del nombre de la fuente de datos
data_source_name = "my_data_source"

try:
    # Se obtiene la fuente de datos del contexto
    data_source = context.data_sources.get(data_source_name)
except KeyError:
    # Si no existe, se agrega la fuente de datos al contexto
    data_source = context.data_sources.add_pandas(name=data_source_name)


# Definición del subconjunto de datos de la fuente de datos
data_asset_name = "my_dataframe_data_asset"

try:
    # Se obtiene el subconjunto de datos de la fuente de datos de la fuente creada
    data_asset = data_source.get_asset(data_asset_name)
except LookupError:
    # Si no existe, se agrega el subconjunto de datos a la fuente de datos
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# Definción del nombre del lote
batch_definition_name = "my_batch_definition"

try:
    batch_definition = (
    context.data_sources.get(data_source_name)
    .get_asset(data_asset_name)
    .get_batch_definition(batch_definition_name)
)
except KeyError:
    # Se crea la definición del lote
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
    batch_definition_name
)
    
# Se obtiene la ruta del archivo CSV que es la fuente de datos
csv_path = "data\detalle1.csv"
dataframe = pd.read_csv(csv_path, sep=";", encoding='latin1')

batch_parameters = {"dataframe": dataframe}



# Get the dataframe as a Batch
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

# Create a Validation Definition
definition_name = "my_validation_definition"

try:
    validation_definition = context.validation_definitions.get(definition_name)
except gx.exceptions.exceptions.DataContextError:
    validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name=definition_name
)
    # Add the Validation Definition to the Data Context
    validation_definition = context.validation_definitions.add(validation_definition)

# Run the Validation Definition
validation_results = validation_definition.run(batch_parameters=batch_parameters)

# Review the Validation Results
print(validation_results)

# Create a list of one or more Validation Definitions for the Checkpoint to run
validation_definitions = [
    context.validation_definitions.get("my_validation_definition")
]

# Create a list of Actions for the Checkpoint to perform
action_list = [    
    # This Action updates the Data Docs static website with the Validation
    #   Results after the Checkpoint is run.
    UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]

# Create the Checkpoint
checkpoint_name = "my_checkpoint"


try:
    checkpoint = context.checkpoints.get(checkpoint_name)
except gx.exceptions.exceptions.DataContextError:
    checkpoint = gx.Checkpoint(
    name=checkpoint_name,
    validation_definitions=validation_definitions,
    actions=action_list,
    result_format={"result_format": "BASIC"},
)
    # Save the Checkpoint to the Data Context
    context.checkpoints.add(checkpoint)

# Run the Checkpoint
validation_results = checkpoint.run(
    batch_parameters=batch_parameters
)

context.open_data_docs()