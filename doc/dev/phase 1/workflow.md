## Phase one
The goal is to build a python application that consecutively invokes three functions:
- ingest
- transform
- load

Each function generates data in an agreed format (see data flow) that is passed to the next finction.
When executed, the application populates a local warehouse DB  with data from the ToteSys DB.
Each function and ther internal functions are tested for at least a happy path.
