# Python should read the .csv file
import csv
#import os
import json
#file_path = "input.csv"
#print("Trying to open:", file_path)
#print("Exists?", os.path.exists(file_path))
#try:
 #   with open(file_path,"r") as file:
  #      content = csv.reader(file)
  #      for line in content:
  #          if not line:
   #             continue
    #        print(line)
#except FileNotFoundError:
 #   print("That file was not found, check file extension")
#except PermissionError:
 #   print("You donot have permission to read that file ")


def csvConvert(csv_path, json_path):
    jsonData = {}
    
    with open(csv_path, encoding='utf-8') as csvfile:
        
        csvData = csv.DictReader(csvfile)
        
        for rows in csvData:
            key = rows['timestamp']
            jsonData[key] = rows
            
    with open(json_path, 'w', 'utf-8') as jsonfile:
        jsonfile.write(json.dumps(jsonData, indent=2))
    print ("DataConverted")
    
csv_path = 'energy_data.csv'

json_path = 'energy_data.json'

csvConvert(csv_path, json_path)
