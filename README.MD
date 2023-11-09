# Parser for parsing the Open Log Replicator output file written in Python

Background:

OpenLogReplicator is a GPL-licensed project created by Adam Leszczyński <aleszczynski@bersler.com> hosted on GitHub. It is the only available Open Source change capture component available for an Oracle database which uses binary reading of redo log files. The main target of the output is Kafka, but it can be easily extended to other systems.

This is a repository for a parser that parses the OpenLogReplicator output file, creates Create, Read, Update, Delete(CRUD) queries based on it, and executes those queries connecting to the target PostgreSQL database.

Features:

- Parsing Open Log Replicator output file
- Connecting to the PostgreSQL database
- Creating the table in PostgreSQL from Python
- Defining the CRUD queries based on the Open Log Replicator output file
- Performing the queries on target database

### Cloning the repository

```shell
git clone https://github.com/jasarevicarijan/openlr-output-parser.git
```

### Install packages

```shell
pip install psycopg2
```

### Setup the PostgreSQL connection parameters

```py
db_params = {
    'host': 'your_host_name',
    'database': 'your_database_name',
    'user': 'your_user',
    'password': 'your_password'
}
```

### Start the app

```shell
python parser.py
```
