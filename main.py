import os
import yaml
import psycopg2
import csv

# Load credentials from.credentials.yaml
with open('safetynet-prod/.credentials.yaml', 'r') as f:
    credentials = yaml.safe_load(f)

# Connect to the database
conn = psycopg2.connect(
    host=credentials['database']['endpoint'],
    database=credentials['database']['name'],
    user=credentials['database']['username'],
    password=credentials['database']['password']
)
cur = conn.cursor()

# Define the queries and file names
queries = [
    ('SELECT * FROM ecomp_forms', 'formdata'),
    ('SELECT * FROM ecomp_site_hierarchy', 'sites'),
    ('SELECT * FROM ecomp_templates', 'templates'),
    ('SELECT * FROM ecomp_user_details', 'userdetails'),
    ('SELECT * FROM ecomp_users_training_details', 'trainingdetails'),
    ('SELECT * FROM users', 'users')
]

# Run each query and save the results to a file
for query, filename in queries:
    cur.execute(query)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    
    with open(os.path.join('safetynet-prod', f'{filename}.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)  # write the column names as the first row
        writer.writerows(rows)

# Close the database connection
cur.close()
conn.close()