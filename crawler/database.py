import sqlite3

class Database:
	DB_FILE = "./data/ytdata.db"
	COLUMNS = (
		"id",
		"views",
		"rating",
		"rates",
		"date_published",
		"length",
		"referred_by",
		"title",
		"traversed",)
	TABLE_NAME = "vidtable1"
	
	def __init__(self):
		self.connection = sqlite3.connect(self.DB_FILE)
		self.conn = self.connection
	
	def create_table(self, num="1"):
		self.conn.execute("CREATE TABLE vidtable%s(%s)" % (num, ",".join(self.COLUMNS)))

	
	def close(self):
		self.connection.close()

if __name__ == "__main__":
	db = Database()
	db.create_table()
	db.close()


