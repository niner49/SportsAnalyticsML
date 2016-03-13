#from pymongo import MongoClient
import pprint
import pymongo


#client = pymongo.MongoClient("mongodb://localhost:27017")
#db = client.nba

# Connection attributes
hostname = 'localhost'
port = 27017
database = 'nba'

# Connect to DB
client = pymongo.MongoClient(hostname,port)
db = client[database]


# Function will reformat JSON into more readable format and store into playerBoxScore collection
def playerCollectionReformat():
    result = db.orig.aggregate([
    {"$project":{
                "resultSets1":{"$arrayElemAt":["$resultSets", 0]},
                "resultSets2":{"$arrayElemAt":["$resultSets", 4]}
                }
    },
    {"$unwind":"$resultSets1.rowSet"},
    {"$unwind":"$resultSets2.rowSet"},
    {"$project":{
                "_id":0,
                "DATE":{"$arrayElemAt":["$resultSets1.rowSet",0]},
                "HOME_TEAM_ID":{"$arrayElemAt":["$resultSets1.rowSet",6]},
                "SEASON":{"$arrayElemAt":["$resultSets1.rowSet",8]},
                "GAME_ID":{"$arrayElemAt":["$resultSets2.rowSet",0]},
                "TEAM_ID":{"$arrayElemAt":["$resultSets2.rowSet",1]},
                "TEAM_ABBREVIATION":{"$arrayElemAt":["$resultSets2.rowSet",3]},
                "PLAYER_ID":{"$arrayElemAt":["$resultSets2.rowSet",4]},
                "PLAYER_NAME":{"$arrayElemAt":["$resultSets2.rowSet",5]},
                "START_POSITION":{"$arrayElemAt":["$resultSets2.rowSet",6]},
                "MIN":{"$arrayElemAt":["$resultSets2.rowSet",8]},
                "FGM":{"$arrayElemAt":["$resultSets2.rowSet",9]},
                "FGA":{"$arrayElemAt":["$resultSets2.rowSet",10]},
                "FG_PCT":{"$arrayElemAt":["$resultSets2.rowSet",11]},
                "FG3M":{"$arrayElemAt":["$resultSets2.rowSet",12]},
                "FG3A":{"$arrayElemAt":["$resultSets2.rowSet",13]},
                "FG3_PCT":{"$arrayElemAt":["$resultSets2.rowSet",14]},
                "FTM":{"$arrayElemAt":["$resultSets2.rowSet",15]},
                "FTA":{"$arrayElemAt":["$resultSets2.rowSet",16]},
                "FT_PCT":{"$arrayElemAt":["$resultSets2.rowSet",17]},
                "OREB":{"$arrayElemAt":["$resultSets2.rowSet",18]},
                "DREB":{"$arrayElemAt":["$resultSets2.rowSet",19]},
                "REB":{"$arrayElemAt":["$resultSets2.rowSet",20]},
                "AST":{"$arrayElemAt":["$resultSets2.rowSet",21]},
                "STL":{"$arrayElemAt":["$resultSets2.rowSet",22]},
                "BLK":{"$arrayElemAt":["$resultSets2.rowSet",23]},
                "TO":{"$arrayElemAt":["$resultSets2.rowSet",24]},
                "PF":{"$arrayElemAt":["$resultSets2.rowSet",25]},
                "PTS":{"$arrayElemAt":["$resultSets2.rowSet",26]},
                "PLUS_MINUS":{"$arrayElemAt":["$resultSets2.rowSet",27]},
                 }
    },
    {"$out":"test"}
   ])


# Function will reformat JSON into more readable format and store into another collection
def boxScoreReformat():
    result = db.sigopt.aggregate([
    {"$project":{
		"resultSets1":{"$arrayElemAt":["$resultSets", 0]},
		"resultSets2":{"$arrayElemAt":["$resultSets", 5]}
		}
    },						
    {"$unwind":"$resultSets1.rowSet"},
    {"$unwind":"$resultSets2.rowSet"},
    {"$project":{ 
		"_id":0,
		"DATE":{"$arrayElemAt":["$resultSets1.rowSet",0]}, 
		"HOME_TEAM_ID":{"$arrayElemAt":["$resultSets1.rowSet",6]}, 
		"SEASON":{"$arrayElemAt":["$resultSets1.rowSet",8]}, 
		"GAME_ID":{"$arrayElemAt":["$resultSets2.rowSet",0]}, 
		"TEAM_ID":{"$arrayElemAt":["$resultSets2.rowSet",1]}, 
		"TEAM_NAME":{"$arrayElemAt":["$resultSets2.rowSet",2]}, 
		"TEAM_ABBREVIATION":{"$arrayElemAt":["$resultSets2.rowSet",3]}, 
		"TEAM_CITY":{"$arrayElemAt":["$resultSets2.rowSet",4]}, 
		"MIN":{"$arrayElemAt":["$resultSets2.rowSet",5]}, 
		"FGM":{"$arrayElemAt":["$resultSets2.rowSet",6]}, 
		"FGA":{"$arrayElemAt":["$resultSets2.rowSet",7]}, 
		"FG_PCT":{"$arrayElemAt":["$resultSets2.rowSet",8]}, 
		"FG3M":{"$arrayElemAt":["$resultSets2.rowSet",9]}, 
		"FG3A":{"$arrayElemAt":["$resultSets2.rowSet",10]}, 
		"FG3_PCT":{"$arrayElemAt":["$resultSets2.rowSet",11]}, 
		"FTM":{"$arrayElemAt":["$resultSets2.rowSet",12]}, 
		"FTA":{"$arrayElemAt":["$resultSets2.rowSet",13]}, 
		"FT_PCT":{"$arrayElemAt":["$resultSets2.rowSet",14]}, 
		"OREB":{"$arrayElemAt":["$resultSets2.rowSet",15]}, 
		"DREB":{"$arrayElemAt":["$resultSets2.rowSet",16]}, 
		"REB":{"$arrayElemAt":["$resultSets2.rowSet",17]}, 
		"AST":{"$arrayElemAt":["$resultSets2.rowSet",18]}, 
		"STL":{"$arrayElemAt":["$resultSets2.rowSet",19]}, 
		"BLK":{"$arrayElemAt":["$resultSets2.rowSet",20]}, 
		"TO":{"$arrayElemAt":["$resultSets2.rowSet",21]}, 
		"PF":{"$arrayElemAt":["$resultSets2.rowSet",22]}, 
		"PTS":{"$arrayElemAt":["$resultSets2.rowSet",23]}, 
		"PLUS_MINUS":{"$arrayElemAt":["$resultSets2.rowSet",24]}, 
		 } 
    },
    {"$project":{ 
		"DATE":1,
		"SEASON":1,
		"GAME_ID":1,
		"TEAM_ID":1, 
		"TEAM_NAME":1, 
		"TEAM_ABBREVIATION":1, 
		"TEAM_CITY":1, 
		"MIN":1, 
		"FGM":1, 
		"FGA":1, 
		"FG_PCT":1, 
		"FG3M":1, 
		"FG3A":1, 
		"FG3_PCT":1, 
		"FTM":1, 
		"FTA":1, 
		"FT_PCT":1, 
		"OREB":1, 
		"DREB":1, 
		"REB":1, 
		"AST":1, 
		"STL":1, 
		"BLK":1, 
		"TO":1, 
		"PF":1, 
		"PTS":1, 
		"PLUS_MINUS":1, 
		"WHERE":{
		   "$cond": {"if": {"$eq" :["$TEAM_ID","$HOME_TEAM_ID"] }, "then":"HOME", "else":"AWAY"}
			},
		"PTSA":{"$subtract":["$PTS","$PLUS_MINUS"]}
		 } 
    },
    {"$out":"test"}
   ])

    return result

if __name__ == '__main__':
	result1 = boxScoreReformat()
	for document in result1:
		print(document)
        #       exit()


