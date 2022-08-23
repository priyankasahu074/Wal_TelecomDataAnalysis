import pyodbc
import os
import json
count =0
allAttrs = []
errorcount =0
allData = dict()

mydb = pyodbc.connect('Driver={SQL Server};'
                      'Server=devodsread.citrite.net;'
                      'Database=bpmanalytics;'
                      'Trusted_Connection=yes;')

mycursor = mydb.cursor()
callDatabulk = []
locationDatabulk = []
applicationDatabulk = []
wifiDatabulk = []
smsDatabulk = []
bleDatabulk = []
actDatabulk = []
picDatabulk = []
countroot = 0
filecount = 0


for root, dirs, files in os.walk(r'C:\\Users\\priyankaku\\Downloads\\UbiqLog4UCI'):
    filecount = 0
    print ("Folder", countroot)
    countroot = countroot + 1
    wifiDatabulk = []
    smsDatabulk = []
    bleDatabulk = []
    actDatabulk = []
    picDatabulk = []
    for file in files:
        if file[0] != 'l' :
            continue
        try: 
            with open(os.path.join(root, file), "rb") as auto:
                alllines = auto.readlines()
                filenamepath = root.split('\\')
                filenamepathlen = len(filenamepath)
                user = filenamepath[filenamepathlen-1]
                # print(lineone)
                for line in alllines:
                    jsonline = json.loads(line)
                    for attr in jsonline:
                        # print(attr)
                        tmp = allData.get(attr)
                       
                        # if tmp is None:
                        #     allData[attr] = list(jsonline[attr].keys())
                        if(attr == 'Call'):
                            callDatabulk.append((user, file, jsonline[attr]['Number'], jsonline[attr]['Duration'], jsonline[attr]['Time'], jsonline[attr]['Type']))                           
                            # allAttrs.append(attr)
                        if(attr == 'Location'):
                            locationDatabulk.append((user, file, jsonline[attr]['Latitude'], jsonline[attr]['Longtitude'], jsonline[attr]['Altitude'], jsonline[attr]['time'],jsonline[attr]['Accuracy'],jsonline[attr]['Provider'],jsonline[attr]['Speed']))                           
                        if(attr == 'Application'):
                            applicationDatabulk.append((user,file, jsonline[attr]['ProcessName'], jsonline[attr]['Start'], jsonline[attr]['End']))                           
                        if(attr == 'WiFi'):
                            wifiDatabulk.append((user,file, jsonline[attr]['SSID'], jsonline[attr]['BSSID'], jsonline[attr]['capabilities'], jsonline[attr]['level'],jsonline[attr]['frequency'],jsonline[attr]['time']))       
                        # if(attr == 'SMS'):
                          #  smsDatabulk.append((user,file, jsonline[attr]['Address'], jsonline[attr]['date'], jsonline[attr]['body'], jsonline[attr]['Type'],jsonline[attr]['metadata'])) 
                        if(attr == 'Bluetooth'):
                            bleDatabulk.append((user, file,jsonline[attr]['name'], jsonline[attr]['address'], jsonline[attr]['bond status'], jsonline[attr]['time'])) 
                        if(attr == 'Activity'):
                            actDatabulk.append((user,file, jsonline[attr]['start'], jsonline[attr]['end'], jsonline[attr]['type'], jsonline[attr]['condfidence'])) 
                        if(attr == 'Picture'):
                            picDatabulk.append((user, file,jsonline[attr]['FullPath'], jsonline[attr]['Time']))                           
                                        
        except Exception as e:
            errorcount=errorcount+1
            # print("error",str(e))
        
       
        try:
            filecount = filecount +1
            print("For file: ", file, "(",filecount,")", "In Folder: ", root, "(", countroot,")" )
            sql1 = "INSERT INTO wal_CallData6 (Username,filename,Number, Duration, Time, Type) VALUES (?,?,?,?,?,?)"
            mycursor.executemany(sql1, callDatabulk)
            mydb.commit()

            print("Call Data done")

            sql2 = "INSERT INTO Wal_LocationData (Username,filename,Latitude, Longtitude, Altitude, time, Accuracy, Provider, Speed ) VALUES (?,?, ?,?,?,?, ?,?,?)"
            mycursor.executemany(sql2, locationDatabulk)
            mydb.commit()

            print("Location Data done")


            sql3 = "INSERT INTO ApplicationData (Username,filename,ProcessName, StartTime, EndTime) VALUES (?,?,?,?,?)"
            mycursor.executemany(sql3, applicationDatabulk)
            mydb.commit()

            print("Application Data done")


            sql4 = "INSERT INTO WifiData (Username,filename,SSID, BSSID, capabilities, level, frequency, time ) VALUES (?,?,?,?,?,?,?,?)"
            mycursor.executemany(sql4, wifiDatabulk)
            mydb.commit()

            print("Wifi Data done")


            # sql5 = "INSERT INTO SmsData (Username,Address, date, body, Type, metadata ) VALUES (?,?,?,?,?,?)"
            # mycursor.executemany(sql5, smsDatabulk)
            # mydb.commit()

            # print("Sms Data done")



            sql6 = "INSERT INTO BleData (Username,filename,name, address, bondStatus, time ) VALUES (?,?,?,?,?,?)"
            mycursor.executemany(sql6, bleDatabulk)
            mydb.commit()

            print("BLE Data done")


            sql7 = "INSERT INTO ActivityData (Username,filename,startTime, endTime, type, condfidence ) VALUES (?,?,?,?,?,?)"
            mycursor.executemany(sql7, actDatabulk)
            mydb.commit()

            print("Activity Data done")


            sql8 = "INSERT INTO PicData (Username,filename,FullPath, Time) VALUES (?,?,?,?)"
            mycursor.executemany(sql8, picDatabulk)
            mydb.commit()

            print("PIC Data done")
            print("\n")

            
        except Exception as e:
            errorcount = errorcount +1

print ("Error Count", errorcount)
# print(list(set(allAttrs)))

# print(allData)
# print(errorcount)
