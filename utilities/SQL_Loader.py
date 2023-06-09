import os


def getQuery(queryName, directory="config/sql_queries"):
    """
    Load all SQL queries from files stored in a given directory.

    :param queryName: Name of query that we are looking for
    :param directory: Directory containing the SQL files.
    :return: Query keyed by filename.
    """
    queries = {}
    for filename in os.listdir(directory):
        if filename.endswith('.sql'):
            with open(os.path.join(directory, filename), 'r') as file:
                # Use the filename without extension as the key
                key = os.path.splitext(filename)[0]
                queries[key] = file.read()
    return queries[queryName]
