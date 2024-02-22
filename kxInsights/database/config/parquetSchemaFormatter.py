import os
import pyarrow.parquet as pq

kxTypes = {'decimal128(37, 19)':'float',
           'uint32':'int',
           'int64':'long',
           'string':'symbol',
           'uint64':'long',
           'uint32':'int',
           'bool':'boolean'
          }

# base_dir = r'C:\Users\jdkal\maystreet\lse_cash_l2_full_depth_parquet'

def cvEntry(fname,ftype): 
    if 'Timestamp' in fname: ft = 'timestamp'
    else: ft = ftype
    return {"type": ft,"attrDisk": "","attrMem": "","attrOrd": "","name": fname,"foreign": ""}

def schema_from_parquet(filename):
    schema = pq.read_schema(filename)
    cvColumns = [cvEntry(field.name,kxTypes[str(field.type)]) for field in schema]
    return cvColumns

def table_creator(tableName, columns, timeCol):
    js = {
        "columns": columns,
        "primaryKeys": [], 
        "type": "partitioned",
        "prtnCol": timeCol,
        "name": tableName,
        "sortColsDisk": [timeCol],
        "sortColsMem": [timeCol],
        "sortColsOrd": [timeCol]
    }
    return js
    
def multiple_time_cols(timeCols):
    input_text = ""
    for i in enumerate(timeCols):
        input_text = f'{input_text}{i[0]}: {i[1]}\n'
    val = input(input_text)
    return timeCols[int(val)]

def pick_time(tablename, columns):
    timeCols = [i['name'] for i in columns if i['type'] == 'timestamp']
    if len(timeCols) == 0: 
        print(f'{tablename} There are no columns with datatype timestamp, ENDING')
        return
    elif len(timeCols) == 1:
        return timeCols[0]
    elif len(timeCols) > 1:
        print(f'Table `{tablename}` has multiple time columns choose one to partition below:')
        return multiple_time_cols(timeCols)
    else:
        print('Unknown Error')
        return

def fromParquet(directory):
    files = os.listdir(directory)
    jsonFile = []
    for file in files:
        fn = file.split('.')[0].replace('_','')
        data=schema_from_parquet(f'{directory}\\{file}')
        timeCol = pick_time(fn, data)
        print(timeCol)
        tbl = table_creator(fn, data, timeCol)
        jsonFile.append(tbl)
    return jsonFile
