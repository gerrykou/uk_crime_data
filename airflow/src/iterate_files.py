import os

print(os.getcwd())
print(os.listdir())

for file in os.listdir():
     if 'parquet' in file:
             print(file)
