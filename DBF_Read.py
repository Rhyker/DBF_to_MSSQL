import shapefile


# Reads .dbf lines and generator zips together field names and records.
def records(filename):

    reader = shapefile.Reader(dbf=open(filename, 'rb'))
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    for sr in reader.iterRecords():
        yield dict(zip(field_names, sr))


# Reads the .shp and .dbf at the same time, adds .dbf data to dict and then adds reprojected .shp coordinates to dict.
def shape_records(filename):

    reader = shapefile.Reader(filename)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]

    for sr in reader.iterShapeRecords():
        zipped_dict = dict(zip(field_names, sr.record))
        zipped_dict['COORDINATES'] = sr.shape.points[0]
        yield zipped_dict

