import json
import collections
import psycopg2

conn_string = "host='localhost' dbname='subreddits' user='postgres' password={os.enciron.get('db_password')}"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()
cursor.execute("SELECT * FROM subreddits")
rows = cursor.fetchall()

subreddits = set()
for row in rows:
    #if len(subreddits) > 100:
    #    break
    subreddits.add(row[0])

sub_arr = []
for sub in subreddits:
    sub_arr.append({'id': sub, 'label': sub })
links_arr = []

for row in rows:
    if row[0] not in subreddits or row[1] not in subreddits:
        continue
    links_arr.append({'target': row[0], 'source': row[1], 'strength': row[2]})

result = {
    "nodes": sub_arr,
    "links": links_arr
}

with open('result_large.json', 'w') as f:
    f.write(json.dumps(result))
