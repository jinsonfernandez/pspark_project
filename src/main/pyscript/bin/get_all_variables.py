import os

# import pprint as pp
# pp.pprint(dict(os.environ)['JAVA_HOME'])

# Set Environmen Variable
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

# Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

# Set other Variable
appName = "USA Prescriber Research Report"
current_path = os.getcwd()
# print(current_path)
staging_dim_city = f"{current_path}\\src\\main\\pyscript\\staging\\dimension_city"  # Staging is at same level as bin so going one step back
staging_fact = f"{current_path}\\src\\main\pyscript\\staging\\fact"

for file in os.listdir(staging_dim_city):
    file_dir = f"{staging_dim_city}\\{file}"

