from pysqlite2 import dbapi2 as sqlite3
import csv
try:
    from settings import db_location
except:
    from pyzipcode.settings import db_location

conn = sqlite3.connect(db_location)
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS ZipCodes;")
c.execute("CREATE TABLE ZipCodes(zip VARCHAR(5), insee VARCHAR(5), city TEXT, ad1 TEXT, ad2 TEXT, longitude DOUBLE, latitude DOUBLE);")
c.execute("CREATE INDEX zip_index ON ZipCodes(zip);")
c.execute("CREATE INDEX insee_index ON ZipCodes(insee);")
c.execute("CREATE INDEX city_index ON ZipCodes(city);")
c.execute("CREATE INDEX ad1_index ON ZipCodes(ad1);")
c.execute("CREATE INDEX ad2_index ON ZipCodes(ad2);")

reader = csv.reader(open('zipcode_fr.csv', "rb"))
reader.next() # prime it

for row in reader:
    zip = row[0]
    insee = row[1]
    city = row[6]
    ad1 = row[8]
    ad2 = row[10]
    longitude = row[11]
    latitude = row[12]

    if not longitude:
        longitude = 'null'
    if not latitude:
        latitude = 'null'

    query = 'INSERT INTO ZipCodes values("%s", "%s", "%s", "%s", "%s", %s, %s)' % (
        zip,
        insee,
        city,
        ad1,
        ad2,
        longitude,
        latitude,
    )

    c.execute(query)

conn.commit()

# We can also close the cursor if we are done with it
c.close()
