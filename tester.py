from streambatch import StreambatchConnection
points = [[3.940705,49.345238]]
connection = StreambatchConnection(open('key.txt').read().strip())
query_id = connection.request_ndvi( points=points)
df = connection.get_data(query_id,debug=True)
print(df.head())