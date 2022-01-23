def psql_insert_copy(table, conn, keys, data_iter):
    """
    Method for bulk loading the data into the SQL database.
    """
    import csv
    from io import StringIO

    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def integrate_source_versions(source, db_url, source_name=None):
    """
    Inspects all the tables (versions) from a source and merges them in a single one.

    Input:
        source that has to be integrated.

    Output:
        main source table in the DBMN.
    """
    import pandas as pd
    from sqlalchemy import create_engine, inspect

    # Initialize the engine and df
    engine = create_engine(db_url)
    conn = engine.connect()
    df = None

    # Get table names
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    # Loop through table names to get the ones relative to the given source
    versions = []
    for table in table_names:
        if source in table and (source != table and source_name != table):
            versions.append(table)
    
    # Concatenate the dataframes in a single version
    df_init = True
    for version in sorted(versions):
        if df_init:
            df = pd.read_sql_table(version, conn)
            df_init = False

        else:
            aux = pd.read_sql_table(version, conn)
            df = pd.concat([df, aux])

    print('Tables from {} integrated into one table'.format(source))

    if not source_name:
        source_name = source

    df.drop_duplicates()
    df.to_sql(source_name, engine, method=psql_insert_copy, if_exists='replace', index = False)
    print('Single source table succesfully loaded into database')

    conn.close()
    return