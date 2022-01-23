from aux_functions import *
from landing_zone.aux_landing import *
from formatted_zone.aux_formatted import *
from trusted_zone.aux_trusted import *
from exploitation_zone.aux_exploitation import *
import os
landing, temporal, persistent, formatted, trusted, exploitation = connect_paths()

# Define parameters

# Landing zone
temporal_files = [x for x in os.listdir(temporal) if x.partition('.')[-1] in ['csv', 'dta' , 'xlsx']]

for file in temporal_files:
    shutil.move(os.path.join(temporal, file), os.path.join(persistent, file))
    rename_csv(filename = file, file_path = persistent)

# Formatted zone
persistent_files = [x for x in os.listdir(persistent) if x.partition('.')[-1] in ['csv', 'dta' , 'xlsx']]
db_url_source = 'postgresql+psycopg2://postgres:root@localhost:5432/adsdb'
output_path = os.path.join(formatted, "profiling")

tables_to_load(persistent_files, persistent, db_url_source, replace=True)
outliers_duplicated_profiling(db_url_source, replace=False, outlier_treatment='na', delete_duplic_rows=True, delete_duplic_cols=False, output_path=output_path)

# Trusted zone
sources = {'wbd': None, 'wgi': None, 'countries': 'countries_dim'}

for source in sources:
    if sources[source]:
        integrate_source_versions(source, db_url_source, source_name=sources[source])
    else:
        integrate_source_versions(source, db_url_source)

# Integrated zone
db_url_schema = 'postgresql+psycopg2://postgres:root@localhost:5432/cluster_countries'
schema_path = os.path.join(exploitation, './schemas/conflict_schema.sql')

create_schema(db_url_schema, schema_path)
populate_schema(db_url_source, db_url_schema)

# Analysis
