# Overall tasks

### Define the project context

- Define context and problem to solve. Do we go for conflicts database?

**Constraint 1.** Your project must consider, at least, two different data sources that you will have 
to integrate during the project.

### Implement the organization data management backbone

- Create a **landing zone** on google drive. It must have a temporal and a persistent folder.
- Create a **formatted zone** with a relational model. Use PostgreSQL. Set up the databases on our computers. As a thule of rumb, create one table for each source version.
- Crete a **trusted zone** with a single table per source. It requires two steps: query over the formatted zone to load the tables, and a set of processes to treat data quality. *PyDeequ, Trifacta, OpenRefine* can be used in this last step.
- Create a **exploitation zone** where the view for the analysis is generated. It is key to implement data integration and data quality processes for integration in here. From trusted to exploitation -> ETL.
- Notebooks between zones should be created to run the transformations.

**Constraint 2.** Organize your notebooks per zones. For example, if you use Google Drive, create 
a folder named notebooks and inside group your notebooks in folders per zone: landing zone, 
formatted zone, trusted zone and exploitation zone.

**Constraint 3.** Use a meaningful name convention for the notebooks and add text and charts 
inside the notebook to explain your processes. You are advised to use Python as prototying 
language.

**Constraint 4.** In the trusted and exploitation zones, each data quality process must be separated 
in its own notebook (unless you use an external tool, then, be sure to talk to your supervisor to 
agree on how to open this information so that it can be evaluated).

**Constraint 5.** Include a notebook per database that explains its structure (schema) and profiles 
the data in that database. This way, the supervisor does not need access to the database.

### Implement, at least, one data analysis backbone

- Create another different database with the **analytical sandbox** required to perform your data analysis. The DBMS used in the previous sections is a good candidate.
- Generate training and validation sets in the **feature generation zone**. It is ok to map variables as features. *Hopsworks, Feature Store, etc.* can be used.
- Model training and validation can be done in Python, R, etc. Do not focus on this part, just do something coherent.


**Constraint 6.** The scripts to generate the analytical sandbox (if you implement it), the feature 
selection, model training and validation must be implemented in notebooks and organized in 
subfolders (e.g., inside an /analysis/ folder). If you use a MLOps tools, be sure to talk to your 
supervisor to agree on how to open this information so that it can be evaluated.

**Constraint 7.** Be sure to include notebooks to scrutinize each data repository you create (follow 
the same criteria as stated in the data management backbone: i.e., schema and profiles). Special 
attention for that storing the training and validation datasets. Be sure the profile information 
you generate there is meaningful for the analysis at hand.

### Explain your decisions and guarantee reproducibility

- Everything done until here corresponds to the prototyping. Henceforth, the operations environment is generated.
  - Generate orchestrated executable code from the notebooks.
  -  Place it into a continuous integration environment (e.g., Gitlab).

^It is optional to develop an integration environment. In a real project we would do it.

^Three aspects that will be positively considered in this layer:
- A simple code monitoring the execution of the model in runtime.
- Enrich the extracted code from the notebooks with error handling, such as basic unit testing.
- Couple a quality control tool to control the quality of your code (e.g., SonarQube) and report about it.

**Constraint 8.** Create a code repository orchestrating all the code from your notebooks able to
generate your operations environment.

**Constraint 9.** Your operations layer must be able to execute and ingest a new data source and 
propagate it throughout all layers, including the execution of the model in runtime upon new 
arrival of data.
