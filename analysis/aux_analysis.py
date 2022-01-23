def preprocess(dataframe, years = [2005, 2009, 2011]):
    """
    Remove rows that only contain NA's, impute the others. Applies dimension reduction to two dimensions. Rename index to countryname and year.
    
    input: dataframe, list of years to analyze
    output: list of preprocessed dataframes per year
    """
    from sklearn import preprocessing
    from sklearn.impute import SimpleImputer
    import numpy as np
    import pandas as pd
    
    df = dataframe.copy()
    df['year'] = df['year'].astype(str)
    df.drop(['time', 'economy', 'code', 'iso', 'region', 'id'], axis=1)
    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()

    keep = df[numeric_cols].dropna(how = 'all').index
    df = df.iloc[keep,]

    df_name_year = df.filter(items=['country_name', 'year']).reset_index(drop=True)
    df_num = df[numeric_cols].reset_index(drop=True)
    scaler = preprocessing.StandardScaler().fit(df_num)
    df_num = pd.DataFrame(scaler.transform(df_num), columns = df_num.columns)

    imp_mean = SimpleImputer(missing_values=np.nan, strategy='mean')
    imp_mean.fit(df_num)
    df_num = pd.DataFrame(imp_mean.transform(df_num), columns = df_num.columns)

    df = df_name_year.join(df_num)

    df["country_year"] = df["year"] + '_' + df["country_name"]
    index = df["country_year"]
    df = df.set_index(index)  

    res = []
    for year in years:
        df_year = df[df['year'] == str(year)]
        df_year = df_year[numeric_cols]
        res.append(df_year)
    

    return res

def cluster(list_of_dataframes, nr_of_clusters = 5, analysis_path='.\analysis'):
    """
    Cluster each dataframe in list.
    input: List of dataframes, number of desired clusters per dataframe
    output: Generates two reports for each year: An interactive html plot with the clustered countries and a text file profiling the PCA dimensions & clusters. 
    """
    import os 
    from sklearn.decomposition import PCA
    from sklearn.cluster import KMeans
    from sklearn.cluster import DBSCAN
    import matplotlib.pyplot as plt
    import plotly.express as px
    import numpy as np
    import pandas as pd

    report_path = os.path.join(analysis_path, "cluster_results")

    for df in list_of_dataframes:
        ### ANALYSIS
        data = df.copy()
        pca = PCA(n_components=2)

        index = df.index
        pca =pca.fit(df)
        X = pca.transform(df)

        pca_res = pd.DataFrame(data = X
                    , columns = ['pc1', 'pc2'])
        
        # Calculate loadings
        loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
        loadings_df = pd.DataFrame(abs(loadings))          
        df = pca_res.set_index(index)

        # Calculate clusters
        kmeans = KMeans(n_clusters=nr_of_clusters, n_init = 50).fit(df)
        df['cluster'] = kmeans.labels_.astype(str)
        df_with_countryname = df.copy()
        df_with_countryname['country'] = df_with_countryname.index

        ### REPORT
        
        #Create plots
        fig = px.scatter(df_with_countryname, x='pc1', y='pc2', color="cluster", text = 'country')
        notable_dim_1 = []
        notable_dim_2 = []
        year = df.index[0].split("_")[0]
        for i, feature in enumerate(data.columns):
            if loadings[i, 0] > loadings_df[0].quantile(0.7): notable_dim_1.append(feature)
            elif loadings[i, 1] > loadings_df[1].quantile(0.7): notable_dim_2.append(feature)
        image_path = report_path + '\{}_cluster_plot.html'.format(year)
        fig.write_html(image_path)
        
        #Create Text Output Report
        txt_path = report_path + '\{}_PCA_report.txt'.format(year)
        f = open(txt_path, "a")
        f.truncate(0)
        print('-----------------------------------------------', file = f)
        print('Notable dimensions for PCA I are: {}'.format(notable_dim_1), file = f)
        print('Notable dimensions for PCA II are: {}'.format(notable_dim_2), file = f)
        print('-----------------------------------------------', file = f)
        for clus in range(nr_of_clusters):
            groups = list(df[df['cluster'] == str(clus)].index)
            mean_pc1 = np.mean(list(df[df['cluster'] == str(clus)].pc1))
            mean_pc2 = np.mean(list(df[df['cluster'] == str(clus)].pc2))
            print("cluster {} : {}".format(clus, groups), file = f)
            print("cluster mean for pc1: {}".format(mean_pc1), file = f)
            print("cluster mean for pc2: {}".format(mean_pc2), file = f)
            print('-----------------------------------------------', file = f)
        print('ATTRIBUTE INFORMARTION:', file = f)
        with open(os.path.join(analysis_path, "metadata.txt")) as f2:
            for line in f2:
                f.write(line)
            print('/n', file = f)
        print('-----------------------------------------------', file = f)
        f.close()