import os
import jinja2 as j2 
from utility.build_models import build_model
from database.postgres import create_pg_engine
from graphlib import TopologicalSorter



def transform_trades():
    
    """ 

    Build all models in the models/ directory 

    """ 


    from graphlib import TopologicalSorter 
    
    # create postgres engine
    engine = create_pg_engine()
  

    # create sorter
    ts = TopologicalSorter()

    # create a dag of models
    for model in os.listdir('models'):
        ts.add(model.replace(".sql", ""))


    dag = tuple(ts.static_order())

    # build each model in the dag
    for model in dag:
        build_model(model, engine=engine)

    



