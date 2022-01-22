import shutil # shutil.move() - It can both move AND rename the file

def connect_paths():
    """
    Connects all the zones
    """
    import os

    main = os.getcwd()
    landing_zone = os.path.join(main, '01_landing_zone')
    temporal = os.path.join(landing_zone, 'temporal')
    persistent = os.path.join(landing_zone, 'persistent')
    formatted_zone = os.path.join(main, '02_formatted_zone')
    trusted_zone = os.path.join(main, '03_trusted_zone')
    exploitation_zone = os.path.join(main, '04_exploitation_zone')

    return landing_zone, temporal, persistent, formatted_zone, trusted_zone, exploitation_zone

def unzip_into(zipname, zip_path=r"./", unzip_path=r"./"):
    """
    Unzips all the contents of the given file into the specified folder.

    Input:
        Zip name. It assumes the zipped file does not contain a folder structure. Additionally, containing folder and destination folder can be specified.

    Output:
        None. The file is already unzipped.
    """
    import zipfile
    from pathlib import Path

    zip_i = Path(zip_path)
    zip_f = Path(unzip_path)
    with zipfile.ZipFile(zip_i / zipname, 'r') as zip_ref:
        zip_ref.extractall(zip_f)
    return

def rename_csv(filename, newname=None, file_path=r"./", add_timestamp=False):
    """
    Self-explanatory. If a newname is not given, it renames the file with the standard form 'name_timestamp' as %Y%m%d.

    Input:
        Filename to be renamed as string. Name that wants to be give. Additionally, the path of the file can be specified. 

    Output:
        None. The file name is already changed.
    """
    from pathlib import Path
    from datetime import datetime
    import os

    if not newname:
        timestamp = datetime.today().strftime('%Y%m%d')
        newname = filename.partition('.')[0] + '_' + timestamp + '.{}'.format(filename.partition('.')[-1])
    
    if newname and add_timestamp:
        timestamp = datetime.today().strftime('%Y%m%d')
        newname = newname.partition('.')[0] + '_' + timestamp + '.{}'.format(filename.partition('.')[-1])

    fpath = Path(file_path)
    os.rename(fpath / filename, fpath / newname)
    return 

def load_database(data_path, db_url):
    """
    Loads the contents of the given csv into a new table in the specified database.

    Input:
        path to .csv, .dta or .xlsx file and database url.
    
    Output:
        Table in the specified database.
    """
    from sqlalchemy import create_engine
    import pandas as pd
    import csv
    import psycopg2
    from io import StringIO
    import os

    # Initialize engine
    engine = create_engine(db_url)
    conn = engine.connect()

    #Extract filename from path
    filename = os.path.basename(data_path)
    
    # Create table from the specified file
    if filename.partition('.')[-1] == 'csv':
        df = pd.read_csv(data_path)
        df.to_sql('{}'.format(filename.partition('.')[0]), engine, if_exists='replace', index = False)
        print('Data succesfully loaded into the database.')
        conn.close()
        
    elif filename.partition('.')[-1] == 'dta':
        df = pd.read_stata(data_path)
        df.to_sql('{}'.format(filename.partition('.')[0]), engine, if_exists='replace', index = False)
        print('Data succesfully loaded into the database.')
        conn.close()
        
    elif filename.partition('.')[-1] == 'xlsx':
        df = pd.read_excel(data_path)
        df.to_sql('{}'.format(filename.partition('.')[0]), engine, if_exists='replace', index = False)
        print('Data succesfully loaded into the database.')
        conn.close()
    

def table_to_profile(lst):
    print('Tables in the database:',", ".join(map(str,lst)))
    answer = input ("choose a table to analyse:")
    while answer not in lst:
        print('Please type valid table name')
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
      
def delete_outliers(dataframe):
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

    rows_containing_outliers = (z_scores <= 3).all(axis=1)
    outlier_index = df_num_no_na[rows_containing_outliers].index.values
    df_no_outliers = dataframe[~dataframe.index.isin(outlier_index)]
    
    return df_no_outliers
    

def outlier_question():
    """
    Asks whether or not outliers have to be deleted
    input: either type: yes or type: no
    output: Boolean
    """
    answer = input ("Do You Want To delete all rows containing outliers? (yes / no) : ")
    while answer not in ['yes', 'no']:
        print('Please type yes or no')
        answer = input ("Do You Want To delete all rows containing outliers? (yes / no) : ")
    if answer == 'yes':
        True
    else:
        False
        
def duplication_question(dataframe):
    """
    Asks whether or not row and/or column duplicates have to be deleted
    input (2x) : either type: yes or type: no
    output: (0,0), (1,0), (0,1) or (1,1) signaling what has to be deleted 
    """
    import pandas as pd
    
    if len(dataframe[dataframe.duplicated()]) == 0 and len([x for x in dataframe.columns.duplicated() if x]) == 0:
        print('no duplicate rows or columns detected')
        return (0,0)
    elif len(dataframe[dataframe.duplicated()]) == 0:
        answer1 = input ("Do You Want To delete all duplicate columns? (yes / no) : ")
        while answer1 not in ['yes', 'no']:
            print('Please type yes or no')
            answer1 = input ("Do You Want To delete all duplicate columns? (yes / no) : ")
        if answer1 == 'yes':
            return (0,1)
        else:
            return (0,0)            
    elif len([x for x in dataframe.columns.duplicated() if x]) == 0:
        answer2 = input ("Do You Want To delete all duplicate rows? (yes / no) : ")
        while answer2 not in ['yes', 'no']:
            print('Please type yes or no')
            answer2 = input ("Do You Want To delete all duplicate rows? (yes / no) : ")
        if answer2 == 'yes':
            return (1,0)
        else:
            return (0,0)   
    else:        
        answer1 = input ("Do You Want To delete all duplicate rows? (yes / no) : ")
        while answer1 not in ['yes', 'no']:
            print('Please type yes or no')
            answer1 = input ("Do You Want To delete all duplicate rows? (yes / no) : ")
        if answer1 == 'yes':
            answer1 = 1
        else:
            answer1 = 0
        answer2 = input ("Do You Want To delete all duplicate columns? (yes / no) : ")
        while answer2 not in ['yes', 'no']:
            print('Please type yes or no')
            answer2 = input ("Do You Want To delete all duplicate columns? (yes / no) : ")
        if answer2 == 'yes':
            answer2 = 1
        else:
            answer2 = 0
        return (answer1, answer2)