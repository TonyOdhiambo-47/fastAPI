"""
CRUD (Create, Read, Update, Delete) Operations with database "tonydb"
First, we use GET requests to get the table names, the columns in each table and the data in a table (3 endpoints)
Next, we use POST, DELETE, PUT and PATCH to add/delete/edit tables, columns and data (9 endpoints)
Lastly, we make a combined summary detailing the tables and columns (1 endpoint) and another also with the number of entries per table (1 endpoint)
"""

# Import Dependencies

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import Dict, List

# SQLalchemy database connection

# Username is postgres, Password is postgres, Database is tonydb
DATABASE_URL = "postgresql://postgres:postgres@localhost/tonydb"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# FastAPI instance
app = FastAPI()


"""
1. Get table names
"""
 
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
        return {f"There are {len(tables)} tables:": tables}


"""
2. Get columns of a specific table
"""

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
    


"""
3. Get all data from a table
"""

@app.get("/tables/{table_name}/data/")
async def get_table_data(table_name: str):
    try:
        with SessionLocal() as session:
            # SQL query to get data from the specified table
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
    



"""
4. Create a new table
"""

# Pydantic model to define the table schema
class TableSchema(BaseModel):
    table_name: str
    columns: dict



# POST Endpoint

@app.post("/tables/")
async def create_table(schema: TableSchema):
    try:
        with SessionLocal() as session:
            # CREATE TABLE query
            columns_definition = ", ".join([f"{col} {dtype}" for col, dtype in schema.columns.items()])
            query = text(f"CREATE TABLE IF NOT EXISTS {schema.table_name} ({columns_definition})")
            
            # Execute the query
            session.execute(query)
            session.commit()
        
        return {"message": f"Table '{schema.table_name}' created successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

# In Postman we will type JSON like this:
{
    "table_name": "table7",
    "columns": {
       "name" : "Tony"
}
}
# Having columns and data at this stage is optional, but the dictionary must be there, even if it is empty




"""
5. Delete a table from the database
"""


@app.delete("/tables/{table_name}/")
async def delete_table(table_name: str):
    try:
        with SessionLocal() as session:
            # DROP TABLE query
            query = text(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            
            # Execute the query
            session.execute(query)
            session.commit()
        
        return {"message": f"Table '{table_name}' deleted successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))




"""
6. Rename existing table
We use PUT because we are updating the entire resource
"""


# Pydantic model to define the rename schema
class RenameTableSchema(BaseModel):
    old_table_name: str
    new_table_name: str




@app.put("/tables/rename/")
async def rename_table(schema: RenameTableSchema):
    try:
        with SessionLocal() as session:
            # ALTER TABLE {} RENAME TO {} query
            query = text(f"ALTER TABLE {schema.old_table_name} RENAME TO {schema.new_table_name}")
            
            # Execute the query
            session.execute(query)
            session.commit()
        
        return {"message": f"Table '{schema.old_table_name}' renamed to '{schema.new_table_name}' successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))

# In Postman we will type the JSON Request body like this:

{
    "old_table_name": "table_4",
    "new_table_name": "table4"
}



"""
7. Create columns in a table
"""

class AddColumnsSchema(BaseModel):
    columns: Dict[str, str]  # column name and data type

# POST Endpoint
@app.post("/tables/{table_name}/add_columns/")
async def add_columns(table_name: str, schema: AddColumnsSchema):
    try:
        with SessionLocal() as session:
            # ALTER TABLE {} ADD COLUMN {} query
            columns_definition = ", ".join([f"ADD COLUMN {col} {dtype}" for col, dtype in schema.columns.items()])
            query = text(f"ALTER TABLE {table_name} {columns_definition}")
            
            # Execute the query
            session.execute(query)
            session.commit()
        
        return {"message": f"Columns {', '.join(schema.columns.keys())} added to table '{table_name}' successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


# In Postman we will type the JSON Request body like this:
{
    "columns": {
        "id": "INTEGER",
        "name": "VARCHAR(50)"
    }
}


"""
8. Delete columns in a table
"""

# Pydantic model for the request body
class DeleteColumnsSchema(BaseModel):
    columns: List[str]

# DELETE Endpoint

@app.delete("/tables/{table_name}/delete_columns/")
async def delete_columns(table_name: str, schema: DeleteColumnsSchema):
    try:
        with SessionLocal() as session:
            # ALTER TABLE {} DROP COLUMN {} query
            for column in schema.columns:
                query = text(f"ALTER TABLE {table_name} DROP COLUMN {column}")
                session.execute(query)
            
            session.commit()
        
        return {"message": f"Columns deleted successfully from table '{table_name}'"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))



# In Postman we will type the JSON Request body like this:
{
    "columns": [
        "ran3", "ran1"
    ]
}



"""
9. Rename columns in a table
We use PUT because we are updating the entire resource
"""


@app.put("/tables/{table_name}/rename_columns/")
async def rename_table_columns(table_name: str, renames: Dict[str, str]):
    try:
        with SessionLocal() as session:
            # Construct the ALTER TABLE RENAME COLUMN query for each column
            for old_column, new_column in renames.items():
                query = text(f"ALTER TABLE {table_name} RENAME COLUMN {old_column} TO {new_column}")
                # Execute the query
                session.execute(query)
            session.commit()
        
        return {"message": f"Columns in table '{table_name}' renamed successfully"}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=str(e))


# In Postman we will type the JSON Request body like this:
{
    "old": "new"
}




"""
10. Add data to a table
"""

# POST endpoint

@app.post("/tables/{table_name}/data/")
def add_data_to_table(table_name: str, data: Dict[str, str]):
    try:
        with SessionLocal() as session:
            # INSERT INTO {}() VALUES () query
            columns = ", ".join(data.keys())
            values = ", ".join([f"'{value}'" for value in data.values()])
            query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({values})")
            
            # Execute the query
            session.execute(query)
            session.commit()
        
        return {"message": f"Data added to the table {table_name} successfully"}

    except Exception as e:
        # If an error occurs, raise an HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail=str(e))


# In Postman we will type the JSON Request body like this:
{
    "number": "4",
    "city": "Guangzhou"
}
# That is assuming the columns are "number" and "city"



"""
11. Delete data in a table
"""

@app.delete("/tables/{table_name}/data/")
def delete_data_from_table(table_name: str, conditions: Dict[str, str]):
    try:
        with SessionLocal() as session:
            # DELETE FROM {table} WHERE {conditions} query
            conditions_clause = " AND ".join([f"{col}='{value}'" for col, value in conditions.items()])
            query = text(f"DELETE FROM {table_name} WHERE {conditions_clause}")
            
            # Execute the query
            result = session.execute(query)
            session.commit()
        
        return {"message": f"Data deleted from the table {table_name} successfully", "rows_affected": result.rowcount}

    except SQLAlchemyError as e:
        # If an error occurs, raise an HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail=str(e))


# In Postman we will type the JSON Request body like this:
{
    "number": "10",
    "city": "Chengdu"
}
# That is assuming the columns are "number" and "city"


"""
12. Update data in a table
We use PATCH because we are only making partial changes to the resource
Like in this case, we only seek to update the name "Guangzhou" (accessed by the "number" 4) to "Guangdong"
"""

# Define the request body model
class UpdateData(BaseModel):
    conditions: Dict[str, str]
    updates: Dict[str, str]

# PATCH request
@app.patch("/tables/{table_name}/data/")
def update_data_in_table(table_name: str, update_data: UpdateData):
    try:
        with SessionLocal() as session:
            # Construct the WHERE clause from conditions
            where_clause = " AND ".join([f"{key}='{value}'" for key, value in update_data.conditions.items()])
            # Construct the SET clause from updates
            set_clause = ", ".join([f"{key}='{value}'" for key, value in update_data.updates.items()])
            # Construct the UPDATE query
            query = text(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}")
            
            # Execute the query
            result = session.execute(query)
            session.commit()
            
            # Check if any row was affected (updated)
            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="No matching record found to update")
        
        return {"message": f"Data in the table '{table_name}' updated successfully"}

    except Exception as e:
        # If an error occurs, raise an HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail=str(e))


# In Postman we will type the JSON Request body like this:
{
    "conditions": {
        "number": "4"
    },
    "updates": {
        "city": "Guangdong"
    }
}




"""
13. Combined Summary: Table names and Column names
"""

@app.get("/tables/summary")
def get_tables_and_columns():
    try:
        with SessionLocal() as session:
            # Query to get all table names
            table_query = text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            table_result = session.execute(table_query)
            tables = [row[0] for row in table_result]
            
            # Dictionary to hold tables and their columns
            tables_summary = {}

            # Query to get columns for each table
            for table_name in tables:
                column_query = text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                    """
                )
                column_result = session.execute(column_query, {"table_name": table_name})
                columns = [row[0] for row in column_result]
                
                # Add table and its columns to the dictionary
                tables_summary[table_name] = columns

            return {"tables_summary": tables_summary}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


"""
14. Combined Summary: Table names and Column names, and the number of entries in each table
"""

@app.get("/tables/summary2")
def get_tables_and_columns():
    try:
        with SessionLocal() as session:
            # Query to get all table names
            table_query = text(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                """
            )
            table_result = session.execute(table_query)
            tables = [row[0] for row in table_result]
            
            # Dictionary to hold tables, their columns, and the number of entries
            tables_summary = {}

            # Query to get columns for each table
            for table_name in tables:
                column_query = text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :table_name
                    """
                )
                column_result = session.execute(column_query, {"table_name": table_name})
                columns = [row[0] for row in column_result]
                
                # Query to get the number of entries for each table
                count_query = text(f"SELECT COUNT(*) FROM {table_name}")
                count_result = session.execute(count_query)
                count = count_result.scalar()

                # Add table, its columns, and the number of entries to the dictionary
                tables_summary[table_name] = {
                    "columns": columns,
                    "number_of_entries": count
                }

            return {"tables_summary": tables_summary}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


# python -m uvicorn tonydb:app --reload
# http://localhost:8000/