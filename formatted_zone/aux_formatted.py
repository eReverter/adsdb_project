def tables_to_load(file_list, file_path, db_url, replace=False):
    """
    Loops through the local files and loads all of the ones that are not in the database. 
    Additionally, if the replace argument is set to True, it will replace the tables with the new ones in case they are duplicated.
    
    Input:
        List of filenames. Database url to the SQL. Replace statement.

    Output:
        Reflected onto the SQL database.  
    """
    from sqlalchemy import create_engine, inspect
    import os

    engine = create_engine(db_url)
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    for file in file_list:
        if replace:
            load_database(os.path.join(file_path, file), db_url)
            print('{} succesfully loaded the specified tables.'.format(file))
        else:
            if file.partition('.')[0] not in table_names:
                load_database(os.path.join(file_path, file), db_url)
                print('{} succesfully loaded the specified tables.'.format(file))
    
    
    return


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

def load_database(filename_path, db_url):
    """
    Loads the contents of the given csv into a new table in the specified database.

    Input:
        Path to .csv, .dta or .xlsx file and database url.
    
    Output:
        Table in the specified database.
    """
    from sqlalchemy import create_engine
    import pandas as pd
    import os

    # Initialize engine
    engine = create_engine(db_url)
    conn = engine.connect()

    #Extract filename from path
    filename = os.path.basename(filename_path)
    
    # Create table from the specified file
    if filename.partition('.')[-1] == 'csv':
        try:
            df = pd.read_csv(filename_path)
        except:
            print('Error in {}. Might be due to error_bad_lines. Skipped them.'.format(filename.partition('.')[0]))
            df = pd.read_csv(filename_path, on_bad_lines='skip')
        
    elif filename.partition('.')[-1] == 'dta':
        df = pd.read_stata(filename_path)
    
    elif filename.partition('.')[-1] == 'xlsx':
        df = pd.read_excel(filename_path)

    else:
        print('There is no data to be loaded.')
        return

    df.to_sql('{}'.format(filename.partition('.')[0]), engine, method=psql_insert_copy, if_exists='replace', index = False)
    print('{} succesfully loaded into the database.'.format(filename.partition('.')[0]))
    conn.close()
    return

def single_table_to_profile(lst):
    print('Tables in the database:',", ".join(map(str,lst)))
    answer = input ("choose a table to analyse:")
    while answer not in lst:
        print('Please type valid table name.')
        answer = input ("choose a table to analyse:")
    return answer

def outlier_overview(dataframe):
    """
    Calculates & plots extreme outliers for numeric attributes. 
    Input: 
    - dataframe: a pandas data frame
    Output:
    - prints percentage of rows that contain extreme outliers from numeric attributes
    - Plots interactive boxplot for all numeric attributes
    """
    import pandas as pd
    import scipy.stats as stats
    import numpy as np
    import matplotlib.pyplot as plt
    from ipywidgets import interactive
    from sqlalchemy import create_engine
    import csv
    import psycopg2
    from io import StringIO
    import os

    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df_num = dataframe.select_dtypes(include=numerics)
    df_num_no_na = df_num.dropna()

    z_scores = abs(stats.zscore(df_num_no_na))

    rows_containing_outliers = (z_scores <= 3).all(axis=1)
    outlier_index = df_num_no_na[rows_containing_outliers].index.values
    df_no_outliers = dataframe[~dataframe.index.isin(outlier_index)]
    
    outlier_percentage = (len(df_no_outliers.index.values)/len(dataframe.index.values))*100
    print('{:.2f} % of rows contain extreme outliers'.format(outlier_percentage))
    
    def box(column):
        plt.boxplot(df_num_no_na[column])
        plt.show()

    interactive_plot = interactive(box, column=df_num_no_na)
    return interactive_plot

def duplication_overview(dataframe):

    import pandas as pd

    row_percentage = len(dataframe[dataframe.duplicated()]) / len(dataframe)*100
    print('{:.2f} % of rows are duplicates'.format(row_percentage))
    
    column_percentage = len([x for x in dataframe.columns.duplicated() if x]) / len(dataframe.columns)*100
    print('{:.2f} % of columns are duplicates'.format(column_percentage))
    
def delete_duplicates(dataframe, to_delete = (0,0)):
    """
    deletes dupiclate rows or columns 
    input: (0,0), (1,0), (0,1) or (1,1) indicating (row, column) and wheather to delete (1) or not (0)
    output: dataframe with specified deleted duplicates.
    """
    import pandas as pd

    if to_delete == (0,0):
        return dataframe
    if to_delete == (1,0):
        return dataframe[~dataframe.duplicated()]
    if to_delete == (0,1):
        return dataframe.loc[:,~dataframe.columns.duplicated()]
    if to_delete == (1,1):
        dataframe = dataframe.loc[:,~dataframe.columns.duplicated()]
        dataframe = dataframe[~dataframe.duplicated()]
        return dataframe
      
def delete_outliers(dataframe, threshold=3):
    """
    Calculates & removes extreme outliers for numeric attributes.
    Input: 
    - dataframe: a pandas data frame
    Output:
    - dataframe without outliers
    """
    import pandas as pd
    import scipy.stats as stats
    import numpy as np
    import matplotlib.pyplot as plt
    from ipywidgets import interactive
    import psycopg2

    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    df_num = dataframe.select_dtypes(include=numerics)
    df_num_no_na = df_num.dropna()

    z_scores = abs(stats.zscore(df_num_no_na))

    rows_containing_outliers = (z_scores <= threshold).all(axis=1)
    outlier_index = df_num_no_na[rows_containing_outliers].index.values
    df_no_outliers = dataframe[~dataframe.index.isin(outlier_index)]
    
    return df_no_outliers

def set_na_outliers(df, threshold=3):
    """
    Calculates & removes extreme outliers for numeric attributes.
    Input: 
    - dataframe: a pandas data frame
    Output:
    - dataframe with outliers as np.nan
    """
    from scipy import stats
    import numpy as np

    num_cols = df.select_dtypes(include=np.number).columns.tolist()
    df[num_cols] = df[num_cols][np.abs(df[num_cols] - df[num_cols].mean()) <= (threshold * df[num_cols].std())]
    
    return df

def outliers_duplicated_profiling(db_url, db_tables=None, replace=False, outlier_treatment='na', delete_duplic_rows=True, delete_duplic_cols=True, output_path="./profiling"):
    """
    Automatically remove the outliers and duplicates of a table. Profile it afterwards. Do this if and only if there is no profiling yet.
    Additionally, the profiling can be redone if the table name is specified and the replace statement set. Bare in mind it will compute the outliers again.
    
    Input:
        Database url. Additionally, specific table names can be provided. Replace indicates wheather or not to replace existing profiling reports.
        Outlier_treatment can be ('na', 'delete', 'keep'). Choose to delete duplicate rows and/or columns.
    Output:
        Profiling as an html. Outlier boxplots and deduplication information TBD.
    """
    from sqlalchemy import create_engine, inspect
    import os
    import pandas as pd
    from pandas_profiling import ProfileReport

    engine = create_engine(db_url)
    conn = engine.connect()
    
    if db_tables:
        pass
    else:
        inspector = inspect(engine)
        db_tables = inspector.get_table_names()

    for table in db_tables:
        if ("profiling_{}.html".format(table)) in os.listdir(output_path) and not replace: # Exists and replace=False
            print('Quality report for {} already exists and is not replaced.'.format(table))
            continue
        else:
            df = pd.read_sql_table(table, conn)
            if outlier_treatment == 'na':
                df = set_na_outliers(df)
            elif outlier_treatment == 'delete':
                df = delete_outliers(df)           
            df = delete_duplicates(df, to_delete=(int(delete_duplic_rows == True),int(delete_duplic_cols == True)))

            profile = ProfileReport(df, title="{}".format(table), minimal = True)
            profile.to_file(os.path.join(output_path, "profiling_{}.html".format(table)))
            print('Quality report for {} succesfully generated.'.format(table))

    conn.close()
    return
        



