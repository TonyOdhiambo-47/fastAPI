# python -m uvicorn test:app --reload
# http://localhost:8000/


# Import Dependencies

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# SQLalchemy database connection

DATABASE_URL = "postgresql://postgres:postgres@localhost/dvdrental"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# FastAPI instance
app = FastAPI()



# Get table names
@app.get("/tables/")
def get_tables():
    with SessionLocal() as session:
        # Execute SQL query to get table names
        query = text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' 
              AND table_type = 'BASE TABLE'
            """
        )
        result = session.execute(query)
        
        # Extract table names from the query result
        tables = [row[0] for row in result]
        
        # Return table names
        return {"tables": tables}



# Get columns of a specific table
@app.get("/tables/{table_name}/columns/")
def get_table_columns(table_name: str):
    with SessionLocal() as session:
        # Execute SQL query to get column names of the specified table
        query = text(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' 
              AND table_name = :table_name
            """
        )
        result = session.execute(query, {"table_name": table_name})
        
        # Extract column names from the query result
        columns = [row[0] for row in result]
        
        # Return column names
        return {"table_name": table_name, "columns": columns}
    



# Define the GET endpoint to retrieve data from the "actor" table
@app.get("/actors/")
async def get_actors():
    try:
        with SessionLocal() as session:
            # Execute SQL query to get data from the "actor" table
            query = text(
                        f"""
                         SELECT *
                         FROM actor
                        """)
            result = session.execute(query)
           
            # Get column names
            columns = ['actor_id', 'first_name', 'last_name', 'last_update']

            # Convert the query result into a list of dictionaries with column names
            actors = [{column: value for column, value in zip(columns, row)} for row in result]

            # Return the list of actors
            return actors

    except Exception as e:
        # If an error occurs, raise an HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail=str(e))
    





# Define the GET endpoint to retrieve data from any other table
@app.get("/tables/{table_name}/data/")
async def get_table_data(table_name: str):
    try:
        with SessionLocal() as session:
            # Execute SQL query to get data from the specified table
            query = text(f"SELECT * FROM {table_name}")
            result = session.execute(query)
           
            # Get column names
            columns = result.keys()

            # Convert the query result into a list of dictionaries with column names
            data = [{column: value for column, value in zip(columns, row)} for row in result]

            # Return the list of data
            return {"table_name": table_name, "data": data}

    except Exception as e:
        # If an error occurs, raise an HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail=str(e))
