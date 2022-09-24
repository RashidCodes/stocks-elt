import logging
import jinja2 as j2
import os




def build_model(model, engine, models_path="models/") -> bool:

    """

    Run models in models directory 


    Parameters
    ----------
    model: str 
        The name of the model (withouth .sql) 

    engine
        Postgres engine 

    models_path: str 
        Directory containing the models 


    
    Returns
    --------
    bool

    """ 

    logging.basicConfig(format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s")


    if f"{model}.sql" in os.listdir(models_path):
        logging.info(f"Building model: {model}")

        # read sql contents into a variable 
        with open(f"models/{model}.sql") as f:
            raw_sql = f.read() 


        # parse the sql using jinja 
        rendered = j2.Template(raw_sql).render(target_table=model, engine=engine)

        # execute parsed sql 
        engine.execute(rendered)

        logging.info(f"Successfully build model: {model}")
        return True 

    else:
        logging.error(f"Could not find model: {model}")
        return False 
